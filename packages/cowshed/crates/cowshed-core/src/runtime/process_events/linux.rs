//! Linux's process event source: one pidfd per registered life in an epoll set, and `/proc`.
//!
//! The one process event Linux gives an unprivileged observer is an exit: a pidfd turns readable
//! when its process has exited (the proc connector, which reports forks and execs, needs
//! `CAP_NET_ADMIN`). So an exit is an event here, and a fork or an exec is learned by looking
//! ([`Source::survey`]), at the boundaries the observer already has: its sampling tick, the
//! start of the job's clock, each custody step, and, for one parent, the exit of a child of its
//! own, which is when a shell or a build tool most often starts its next. No timer runs for it.
//!
//! - A registration is a pidfd, which names its one process whatever later takes the pid, under
//!   an epoll token no other registration is ever given: a queued event of a withdrawn
//!   registration names nothing. The pidfd is also the fence of every read of that life's
//!   `/proc` entry: read while the pidfd says it had not exited, the entry was its own, since no
//!   other process is given the pid of one that has not been reaped.
//! - A fork is a child in `/proc/<pid>/task/<tid>/children`, of any thread, that the last look
//!   did not list. A child forked and gone between two looks is never seen, and a child whose
//!   parent exited before the next look has left every list: the tree of a job says so from its
//!   first moment ([`standing_gap`]), and only a membership read finds such a child again.
//! - An exec is a change of the life's image mark between two looks at `/proc/<pid>/stat`: the
//!   process flag that says it has not exec'd since its fork (`PF_FORKNOEXEC`, shown of every
//!   process), and where the kernel loaded the image (`startcode` and `startstack`, which
//!   address-space randomization moves with every exec). The kernel shows a reader its ptrace
//!   read check refuses the same addresses for every image, so of a life of another user only
//!   the first exec after its fork is seen; an exec onto the same addresses, with randomization
//!   off, is not seen; and of several execs between two looks only the last image is read. A
//!   life that exits between two looks is asked the flag once more of its unreaped entry: last
//!   seen not to have exec'd and shown there to have, it is reported as exec'd with its exit.
//!   Once it is reaped nothing says, and it keeps the image last read.
//! - An exit's wait status is the kernel's exit record of a reaped process
//!   (`PIDFD_GET_INFO` with `PIDFD_INFO_EXIT`, Linux 6.15, which no credential gates), or, of a
//!   process still unreaped, `exit_code` of `/proc/<pid>/stat` where the ptrace read check admits
//!   this reader. Any other exit is reported with its status withheld.
//! - The writer's wake is an eventfd in the same epoll set, which the writer's handle raises.

use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::fs::File;
use std::io::{self, Read as _};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use super::{ExitReport, ObservationPurpose, ObserveError, Report, kernel};
use crate::api::process::ProcessCoverageGap;
use crate::runtime::process_tree::ProcessIdentity;

/// The read of a process's record.
pub(super) const RECORD: &str = "read of /proc/<pid>/stat";
/// The read of the image a process runs.
pub(super) const IMAGE: &str = "read of /proc/<pid>/exe and cmdline";
/// The errno the image read answers a reader it refuses: `exe` is behind the ptrace read check.
pub(super) const IMAGE_REFUSAL: i32 = libc::EACCES;

/// Whether a failed image read is what the kernel answers a reader it refuses. The read names
/// the file it failed on around the kernel's error, so the refusal is told by its kind as well.
pub(super) fn image_refused(error: &io::Error) -> bool {
    error.raw_os_error() == Some(IMAGE_REFUSAL) || error.kind() == io::ErrorKind::PermissionDenied
}

/// The read that lists a process's children.
pub(super) const CHILDREN: &str = "read of /proc/<pid>/task/<tid>/children";
/// The read of the job's group.
pub(super) const GROUP: &str = "read of the process group's members in /proc";
/// The call that signals one life.
pub(super) const SIGNAL: &str = "pidfd_send_signal";
/// The call that registers a life's exit watch.
pub(super) const WATCH: &str = "pidfd_open";
/// What reports a life's wait status once it has exited.
pub(super) const EXIT_STATUS: &str = "PIDFD_GET_INFO";

/// The epoll token of the wake eventfd; registrations count up from 1.
const WAKE: u64 = u64::MAX;

/// What one registration of [`Source`] holds.
#[derive(Debug)]
pub(super) struct Watch {
    /// Names this life alone, and is readable once it has exited.
    pidfd: OwnedFd,
    /// Its token in the epoll set.
    token: u64,
    /// The children its threads listed at the last look, in order.
    children: Vec<u32>,
    /// Where the kernel had loaded its image at the last look that read it.
    mark: Option<ImageMark>,
}

impl Watch {
    /// The pidfd is the fence of every read of this life.
    pub(super) fn held(&self) -> Option<BorrowedFd<'_>> {
        Some(self.pidfd.as_fd())
    }

    /// The watch of a registration a test scripts without a process behind it: the pidfd of
    /// this process, which does not exit under the test, under a token the source never issued.
    #[cfg(test)]
    pub(super) fn scripted() -> Self {
        let own = libc::pid_t::try_from(std::process::id()).expect("this process's pid");
        Self {
            pidfd: pidfd_open(own).expect("a pidfd of this process"),
            token: 0,
            children: Vec::new(),
            mark: None,
        }
    }
}

/// No fork is an event here, so a child that came and went between two looks is never known of:
/// the tree's forks are uncounted from its root's first moment, and it is never proven complete.
pub(super) fn standing_gap(root: u32) -> Option<ProcessCoverageGap> {
    Some(ProcessCoverageGap::UncountedFork { pid: root })
}

/// The pids whose parent is `pid` now: the children of each of its threads. A process that has
/// exited has none.
pub(super) fn children(pid: u32) -> io::Result<Vec<u32>> {
    let mut children = Vec::new();
    list_children(pid, &mut Scratch::default(), &mut children)?;
    Ok(children)
}

/// The buffers the `/proc` reads of one source reuse.
#[derive(Debug, Default)]
struct Scratch {
    path: String,
    text: String,
}

impl Scratch {
    /// Read the file `path` names into `text`; `false` when it names none, or its process went
    /// under the read: the process or thread has gone.
    fn read(&mut self, path: fmt::Arguments<'_>) -> io::Result<bool> {
        self.path.clear();
        self.path.write_fmt(path).map_err(io::Error::other)?;
        self.text.clear();
        match File::open(&self.path).and_then(|mut file| file.read_to_string(&mut self.text)) {
            Ok(_) => Ok(true),
            Err(error) if gone(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

/// Whether a `/proc` read failed because its process or thread no longer exists.
fn gone(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

/// A missing entry says its process has gone only on a mounted procfs.
fn proc_mounted() -> io::Result<()> {
    std::fs::metadata("/proc/self/stat").map(drop)
}

/// Append the children of every thread of `pid` to `children`. The kernel keeps a child on the
/// list of the thread that forked it, and moves it to another thread of the process when that
/// one exits.
fn list_children(pid: u32, scratch: &mut Scratch, children: &mut Vec<u32>) -> io::Result<()> {
    scratch.path.clear();
    write!(scratch.path, "/proc/{pid}/task").map_err(io::Error::other)?;
    let tasks = match std::fs::read_dir(&scratch.path) {
        Ok(tasks) => tasks,
        Err(error) if gone(&error) => return proc_mounted(),
        Err(error) => return Err(error),
    };
    for task in tasks {
        let task = match task {
            Ok(task) => task,
            Err(error) if gone(&error) => break,
            Err(error) => return Err(error),
        };
        let name = task.file_name();
        let Some(thread) = name.to_str() else {
            continue;
        };
        if !scratch.read(format_args!("/proc/{pid}/task/{thread}/children"))? {
            // The thread exited since it was listed.
            continue;
        }
        for child in scratch.text.split_ascii_whitespace() {
            children.push(child.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("process {pid}'s child list names {child:?}, which is no pid"),
                )
            })?);
        }
    }
    Ok(())
}

/// The fields of a `/proc/<pid>/stat` line from its third, the state, on. The command name
/// before them is parenthesized and may hold anything, so they follow its last `)`.
fn stat_fields(pid: u32, stat: &str) -> io::Result<std::str::SplitAsciiWhitespace<'_>> {
    stat.rfind(')')
        .map(|closing| stat[closing + 1..].split_ascii_whitespace())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid}'s stat {stat:?} has no command delimiter"),
            )
        })
}

fn stat_number<T: std::str::FromStr>(pid: u32, field: Option<&str>) -> io::Result<T> {
    field.and_then(|field| field.parse().ok()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid}'s stat is too short, or holds a field that is no number"),
        )
    })
}

/// `PF_FORKNOEXEC` among the flags of a stat line: the process has not exec'd since its fork.
const FORKED_NO_EXEC: u64 = 0x40;

/// What tells the image a process runs from the one it ran before: whether it has exec'd since
/// its fork (`PF_FORKNOEXEC`, which the kernel shows of every process, an exited one included),
/// and where the kernel loaded the image (`startcode` and `startstack`), which address-space
/// randomization moves with every exec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImageMark {
    forked_only: bool,
    code: u64,
    stack: u64,
}

/// The mark of the image `pid` runs now; `None` once the pid names no process, or names one
/// whose leading thread has exited: the kernel shows that thread no image, though other threads
/// of the process may run on in it.
fn image_mark(pid: u32, scratch: &mut Scratch) -> io::Result<Option<ImageMark>> {
    if !scratch.read(format_args!("/proc/{pid}/stat"))? {
        return Ok(None);
    }
    let mut fields = stat_fields(pid, &scratch.text)?;
    if matches!(fields.next(), Some("Z" | "X")) {
        return Ok(None);
    }
    // Fields 9, 26 and 28, the state just read being field 3.
    let flags: u64 = stat_number(pid, fields.nth(5))?;
    let code = stat_number(pid, fields.nth(16))?;
    let stack = stat_number(pid, fields.nth(1))?;
    Ok(Some(ImageMark {
        forked_only: flags & FORKED_NO_EXEC != 0,
        code,
        stack,
    }))
}

/// `pidfd_open`: a descriptor, close-on-exec from birth, that names the process `pid` names now.
fn pidfd_open(pid: libc::pid_t) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes plain integers.
    let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if opened < 0 {
        return Err(io::Error::last_os_error());
    }
    let opened = RawFd::try_from(opened).map_err(io::Error::other)?;
    // SAFETY: a fresh descriptor this function alone owns.
    Ok(unsafe { OwnedFd::from_raw_fd(opened) })
}

/// Whether the process `held` names has exited: its pidfd is readable from then on.
fn exited(held: BorrowedFd<'_>) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd: held.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: one live pollfd entry.
        if unsafe { libc::poll(&raw mut poll, 1, 0) } >= 0 {
            return Ok(poll.revents != 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// What a pidfd says of whether its exited process has been reaped.
enum Reaped {
    /// Not yet: its `/proc` entry is still its own.
    No,
    /// Reaped, and the kernel kept its wait status.
    Status(i32),
    /// Reaped, with no record of how it exited.
    Unrecorded,
}

/// Ask the kernel's exit record of the exited process `held` names. The kernel fills it as the
/// process is reaped, for every holder of a pidfd opened before then, whatever their credentials.
fn reaped(held: BorrowedFd<'_>) -> io::Result<Reaped> {
    // SAFETY: an all-zero pidfd_info is a valid value of the plain C struct.
    let mut info: libc::pidfd_info = unsafe { std::mem::zeroed() };
    info.mask = u64::from(libc::PIDFD_INFO_EXIT);
    // SAFETY: `info` is writable storage of the size the request names.
    if unsafe { libc::ioctl(held.as_raw_fd(), libc::PIDFD_GET_INFO, &raw mut info) } == 0 {
        return Ok(if info.mask & u64::from(libc::PIDFD_INFO_EXIT) != 0 {
            Reaped::Status(info.exit_code)
        } else if info.mask & u64::from(libc::PIDFD_INFO_PID) != 0 {
            Reaped::No
        } else {
            Reaped::Unrecorded
        });
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // Reaped, under a kernel that keeps no exit record (before Linux 6.15).
        Some(libc::ESRCH) => Ok(Reaped::Unrecorded),
        // A kernel without the request (before Linux 6.13). Signal 0 reaches an exited process
        // until it is reaped; one this reader may not signal exists all the same.
        Some(libc::ENOTTY | libc::EINVAL) => {
            // SAFETY: pidfd_send_signal reads a live descriptor and takes no info pointer.
            let sent = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    held.as_raw_fd(),
                    0,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if sent == 0 {
                return Ok(Reaped::No);
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(Reaped::Unrecorded),
                Some(libc::EPERM) => Ok(Reaped::No),
                _ => Err(error),
            }
        }
        _ => Err(error),
    }
}

/// What the entry of an exited, unreaped process shows this reader.
struct Unreaped {
    /// It never exec'd after its fork.
    forked_only: bool,
    /// Its wait status, where the kernel shows it.
    status: Option<i32>,
}

/// The entry of the exited, unreaped process `pid` names; `None` when the pid names no such
/// process.
///
/// `/proc/<pid>/stat` shows `exit_code` to a reader the ptrace read check admits and a 0 to any
/// other, which no field of that line tells apart from an exit with status 0. So the check is
/// asked through a link it gates the same way: an exited process has no working directory, and
/// `cwd` answers an admitted reader `ENOENT` and a refused one `EACCES`.
fn unreaped(pid: u32, scratch: &mut Scratch) -> io::Result<Option<Unreaped>> {
    if !scratch.read(format_args!("/proc/{pid}/stat"))? {
        return Ok(None);
    }
    let mut fields = stat_fields(pid, &scratch.text)?;
    if fields.next() != Some("Z") {
        return Ok(None);
    }
    // Fields 9 and 52, the state just read being field 3.
    let flags: u64 = stat_number(pid, fields.nth(5))?;
    let exit_code: i32 = stat_number(pid, fields.nth(42))?;
    scratch.path.clear();
    write!(scratch.path, "/proc/{pid}/cwd").map_err(io::Error::other)?;
    let shown = match std::fs::read_link(&scratch.path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => true,
        // A process with a working directory is not one that has exited.
        Ok(_) => false,
        Err(error)
            if error.kind() == io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            false
        }
        Err(error) => return Err(error),
    };
    Ok(Some(Unreaped {
        forked_only: flags & FORKED_NO_EXEC != 0,
        status: shown.then_some(exit_code),
    }))
}

/// How the exited process `held` names ended, as far as the kernel shows this observer.
struct Ended {
    /// Its wait status; `None` where the kernel withholds it.
    status: Option<i32>,
    /// Whether it never exec'd after its fork; `None` once it is reaped, when nothing says.
    forked_only: Option<bool>,
}

/// Read how the exited process `held` names, whose pid was `pid`, ended. The `/proc` reads name
/// the pid alone: they are this life's when the pidfd says the life was unreaped after them, and
/// once the life is reaped the kernel's own record answers for its status instead.
fn ended(pid: u32, held: BorrowedFd<'_>, scratch: &mut Scratch) -> io::Result<Ended> {
    let recorded = |status| Ended {
        status,
        forked_only: None,
    };
    match reaped(held)? {
        Reaped::Status(status) => return Ok(recorded(Some(status))),
        Reaped::Unrecorded => return Ok(recorded(None)),
        Reaped::No => {}
    }
    let entry = unreaped(pid, scratch)?;
    Ok(match reaped(held)? {
        Reaped::Status(status) => recorded(Some(status)),
        Reaped::Unrecorded => recorded(None),
        Reaped::No => Ended {
            status: entry.as_ref().and_then(|entry| entry.status),
            forked_only: entry.map(|entry| entry.forked_only),
        },
    })
}

/// The epoll set every registered life's pidfd reports to, and the writer's wake.
#[derive(Debug)]
pub(super) struct Source {
    epoll: OwnedFd,
    /// Raised by the writer's wake handle ([`Source::wake_handle`]); in the set under [`WAKE`].
    wake: OwnedFd,
    /// The life each issued token names until its exit is taken or it is withdrawn.
    tokens: HashMap<u64, ProcessIdentity>,
    /// The next token to issue. None is issued twice.
    next: u64,
    scratch: Scratch,
    /// The children one look lists, kept between looks for its allocation.
    listed: Vec<u32>,
}

impl AsRawFd for Source {
    /// Readable when an event waits for [`Source::collect`].
    fn as_raw_fd(&self) -> RawFd {
        self.epoll.as_raw_fd()
    }
}

impl Source {
    /// Allocate the set before any workload can be forked. A kernel that lists no thread's
    /// children could follow no fork, so it is refused here, not found out later.
    pub(super) fn open() -> Result<Self, ObserveError> {
        std::fs::metadata("/proc/thread-self/children").map_err(|error| {
            kernel(
                CHILDREN,
                0,
                io::Error::new(
                    error.kind(),
                    format!("this kernel lists no thread's children: {error}"),
                ),
            )
        })?;
        // SAFETY: epoll_create1 takes a plain flag and returns a new descriptor.
        let epoll = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll < 0 {
            return Err(kernel("epoll_create1", 0, io::Error::last_os_error()));
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let epoll = unsafe { OwnedFd::from_raw_fd(epoll) };
        // SAFETY: eventfd takes plain integers and returns a new descriptor.
        let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake < 0 {
            return Err(kernel("eventfd", 0, io::Error::last_os_error()));
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let wake = unsafe { OwnedFd::from_raw_fd(wake) };
        let source = Self {
            epoll,
            wake,
            tokens: HashMap::new(),
            next: 1,
            scratch: Scratch::default(),
            listed: Vec::new(),
        };
        source
            .control(
                libc::EPOLL_CTL_ADD,
                source.wake.as_fd(),
                libc::EPOLLIN,
                WAKE,
            )
            .map_err(|error| kernel("epoll_ctl(EPOLL_CTL_ADD)", 0, error))?;
        Ok(source)
    }

    /// A duplicate of the wake eventfd: a count written to it wakes the sole writer.
    pub(super) fn wake_handle(&self) -> io::Result<OwnedFd> {
        self.wake.try_clone()
    }

    /// Register `process` for its exit: the pidfd of the process its pid names now, or `None`
    /// when it names none (`ESRCH`). The image mark read here is what a later look compares.
    pub(super) fn register(
        &mut self,
        process: ProcessIdentity,
    ) -> Result<Option<Watch>, ObserveError> {
        let pid = libc::pid_t::try_from(process.pid).map_err(|error| {
            kernel(
                WATCH,
                process.pid,
                io::Error::new(io::ErrorKind::InvalidInput, error),
            )
        })?;
        let pidfd = match pidfd_open(pid) {
            Ok(pidfd) => pidfd,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(kernel(WATCH, process.pid, error)),
        };
        let mark = image_mark(process.pid, &mut self.scratch)
            .map_err(|error| kernel(RECORD, process.pid, error))?;
        let token = self.next;
        self.next += 1;
        // One shot: an exited process's pidfd stays readable, and its exit is taken once.
        self.control(
            libc::EPOLL_CTL_ADD,
            pidfd.as_fd(),
            libc::EPOLLIN | libc::EPOLLONESHOT,
            token,
        )
        .map_err(|error| kernel("epoll_ctl(EPOLL_CTL_ADD)", process.pid, error))?;
        self.tokens.insert(token, process);
        Ok(Some(Watch {
            pidfd,
            token,
            children: Vec::new(),
            mark,
        }))
    }

    /// Withdraw `process`'s registration from the set. Its token is never issued again, so an
    /// event already queued for it names nothing.
    pub(super) fn withdraw(
        &mut self,
        process: ProcessIdentity,
        watch: &Watch,
    ) -> Result<bool, ObserveError> {
        self.tokens.remove(&watch.token);
        self.control(libc::EPOLL_CTL_DEL, watch.pidfd.as_fd(), 0, 0)
            .map_err(|error| kernel("epoll_ctl(EPOLL_CTL_DEL)", process.pid, error))?;
        Ok(true)
    }

    fn control(
        &self,
        operation: libc::c_int,
        descriptor: BorrowedFd<'_>,
        events: libc::c_int,
        token: u64,
    ) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: events.cast_unsigned(),
            u64: token,
        };
        // SAFETY: one event read from live memory, on a live set and a live descriptor.
        let changed = unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                operation,
                descriptor.as_raw_fd(),
                &raw mut event,
            )
        };
        if changed == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Look at every registered life for what no event reports: the children it has that the
    /// last look did not list, and, where `purpose` reads images, whether its image moved.
    pub(super) fn survey(
        &mut self,
        purpose: ObservationPurpose,
        registered: &mut HashMap<ProcessIdentity, Watch>,
        reports: &mut Vec<Result<Report, ObserveError>>,
    ) {
        let images = purpose == ObservationPurpose::Metadata;
        for (&process, watch) in registered.iter_mut() {
            self.look(process, watch, images, reports);
        }
    }

    /// Take one batch of events, waiting up to `within` for the first (`None`: until one comes):
    /// how many the kernel handed over, each registered life's exit appended to `reports`, and
    /// with it a look at the parent that listed it. An `Err` is the set itself failing.
    pub(super) fn collect(
        &mut self,
        within: Option<Duration>,
        registered: &mut HashMap<ProcessIdentity, Watch>,
        reports: &mut Vec<Result<Report, ObserveError>>,
    ) -> Result<usize, ObserveError> {
        let timeout = match within {
            None => -1,
            // Rounded up to epoll's milliseconds: a wait that ended short of `within` would spin
            // its caller through the rest of it.
            Some(within) => libc::c_int::try_from(within.as_nanos().div_ceil(1_000_000))
                .unwrap_or(libc::c_int::MAX),
        };
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
        let capacity = libc::c_int::try_from(events.len()).unwrap_or(libc::c_int::MAX);
        let ready = loop {
            // SAFETY: `events` is writable for `capacity` events.
            let ready = unsafe {
                libc::epoll_wait(
                    self.epoll.as_raw_fd(),
                    events.as_mut_ptr(),
                    capacity,
                    timeout,
                )
            };
            match usize::try_from(ready) {
                Ok(ready) => break ready,
                Err(_) => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(kernel("epoll_wait", 0, error));
                    }
                }
            }
        };
        // A wake whose count could not be taken is not counted: it would be handed over again
        // at once, and its failure is reported without holding a drain in a loop.
        let mut taken = 0;
        for event in &events[..ready] {
            // `struct epoll_event` is packed on x86-64: copy the field out before using it.
            let token = event.u64;
            if token == WAKE {
                match self.quiet() {
                    Ok(()) => taken += 1,
                    Err(error) => reports.push(Err(error)),
                }
            } else {
                taken += 1;
                if let Some(process) = self.tokens.remove(&token) {
                    self.exit(process, registered, reports);
                }
            }
        }
        Ok(taken)
    }

    /// Take the wake's count, so the eventfd is quiet until the next wake.
    fn quiet(&self) -> Result<(), ObserveError> {
        let mut count = [0_u8; 8];
        // SAFETY: `count` is writable for the eight bytes an eventfd read stores.
        if unsafe {
            libc::read(
                self.wake.as_raw_fd(),
                count.as_mut_ptr().cast(),
                count.len(),
            )
        } < 0
        {
            let error = io::Error::last_os_error();
            // Another wake's read took the count first.
            if error.kind() != io::ErrorKind::WouldBlock
                && error.kind() != io::ErrorKind::Interrupted
            {
                return Err(kernel("read(eventfd)", 0, error));
            }
        }
        Ok(())
    }

    /// Report `process`'s exit with the status the kernel gives this observer, then look at the
    /// parent whose list held it. A life last seen not to have exec'd since its fork, whose
    /// unreaped entry shows it did, exec'd between that look and its exit: its report says so,
    /// and the image of that exec is never read. Once such a life is reaped nothing says
    /// whether it did, and it is reported as last seen.
    fn exit(
        &mut self,
        process: ProcessIdentity,
        registered: &mut HashMap<ProcessIdentity, Watch>,
        reports: &mut Vec<Result<Report, ObserveError>>,
    ) {
        let Some(watch) = registered.get(&process) else {
            return;
        };
        let (ended, failed) = match ended(process.pid, watch.pidfd.as_fd(), &mut self.scratch) {
            Ok(ended) => (ended, None),
            Err(error) => (
                Ended {
                    status: None,
                    forked_only: None,
                },
                Some(error),
            ),
        };
        reports.push(Ok(Report {
            process,
            forked: false,
            exec: watch.mark.is_some_and(|mark| mark.forked_only)
                && ended.forked_only == Some(false),
            exit: Some(
                ended
                    .status
                    .and_then(|status| isize::try_from(status).ok())
                    .map_or(ExitReport::Withheld, ExitReport::Status),
            ),
        }));
        if let Some(error) = failed {
            // The exit is reported all the same; its status is the gap the fold records.
            reports.push(Err(ObserveError::Unread {
                gap: ProcessCoverageGap::UnreadExitStatus { pid: process.pid },
                cause: Box::new(kernel(EXIT_STATUS, process.pid, error)),
            }));
        }
        let parent = registered.iter_mut().find_map(|(&parent, watch)| {
            let at = watch.children.binary_search(&process.pid).ok()?;
            // Forgotten, so a later child given this pid is new to the next look.
            watch.children.remove(at);
            Some((parent, watch))
        });
        if let Some((parent, watch)) = parent {
            self.look(parent, watch, false, reports);
        }
    }

    /// Look at `/proc` for what `process` did since the last look, and report it. The reads name
    /// its pid alone; they are its own when its pidfd says it had not exited after them. One that
    /// has exited is left to its exit event.
    fn look(
        &mut self,
        process: ProcessIdentity,
        watch: &mut Watch,
        images: bool,
        reports: &mut Vec<Result<Report, ObserveError>>,
    ) {
        self.listed.clear();
        if let Err(error) = list_children(process.pid, &mut self.scratch, &mut self.listed) {
            // Whether it forked is unknown; the tree's forks are uncounted already.
            reports.push(Err(ObserveError::Unread {
                gap: ProcessCoverageGap::UncountedFork { pid: process.pid },
                cause: Box::new(kernel(CHILDREN, process.pid, error)),
            }));
            return;
        }
        let mark = if images {
            match image_mark(process.pid, &mut self.scratch) {
                Ok(mark) => mark,
                Err(error) => {
                    reports.push(Err(kernel(RECORD, process.pid, error)));
                    return;
                }
            }
        } else {
            None
        };
        match exited(watch.pidfd.as_fd()) {
            Ok(false) => {}
            Ok(true) => return,
            Err(error) => {
                reports.push(Err(kernel("poll(pidfd)", process.pid, error)));
                return;
            }
        }
        self.listed.sort_unstable();
        self.listed.dedup();
        let forked = self
            .listed
            .iter()
            .any(|child| watch.children.binary_search(child).is_err());
        std::mem::swap(&mut watch.children, &mut self.listed);
        let exec = match (watch.mark, mark) {
            (Some(before), Some(now)) => before != now,
            _ => false,
        };
        if mark.is_some() {
            watch.mark = mark;
        }
        if forked || exec {
            reports.push(Ok(Report {
                process,
                forked,
                exec,
                exit: None,
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::{BufRead, BufReader, Write as _};
    use std::time::Instant;

    use super::super::tests::{
        Scripted, TestFold, argv, held_process, life, marker, record, release, scripted,
        until_exited, watched,
    };
    use super::*;
    use crate::api::dto::ExitStatus;
    use crate::api::process::ProcessCoverage;

    /// The tree of a job watched here says its forks are uncounted from its root's first moment.
    fn uncounted(root: u32) -> ProcessCoverage {
        ProcessCoverage::Gap {
            reason: ProcessCoverageGap::UncountedFork { pid: root },
        }
    }

    /// Two children held on a pipe: no event reports their fork, so nothing holds them until the
    /// observer looks; then each is retained with its pid, parent, exec'd argv and exit.
    #[test]
    fn held_children_are_found_by_a_look_with_their_parent_argv_and_exit() {
        // Each child prints the marker only once it runs cat, then blocks reading fd 3.
        let marker = marker("held");
        let (hold_read, hold_write) = std::io::pipe().unwrap();
        let script = format!(
            "cat {marker} - <&3 & echo $!\n\
             cat {marker} - <&3 & echo $!\n\
             wait"
        );
        let (mut observer, mut child) = watched("bash", &script, Some(hold_read.into()));
        let root = child.id();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let (mut expected, mut running) = (Vec::new(), 0);
        while expected.len() < 2 || running < 2 {
            let mut line = String::new();
            assert_ne!(
                stdout.read_line(&mut line).unwrap(),
                0,
                "the root's output ended"
            );
            match line.trim() {
                "ready" => running += 1,
                pid => expected.push(pid.parse::<u32>().expect("child pid")),
            }
        }
        observer.drain().expect("no event is queued");
        assert_eq!(
            observer.fold().nodes().count(),
            1,
            "no event reports a fork"
        );
        observer
            .sample(Instant::now())
            .expect("no read fails while both children run cat");
        for &pid in &expected {
            let process = observer.fold().live(pid).expect("adopted");
            assert_eq!(
                &*observer.fold().image(process).unwrap().argv,
                argv(&["cat", &marker, "-"]).as_slice()
            );
        }
        drop(hold_write);
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait_bounded().unwrap().success());
        std::fs::remove_file(&marker).unwrap();

        let nodes: Vec<_> = observer.fold().nodes().collect();
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        assert_eq!(nodes[0].identity.pid, root);
        assert_eq!(
            &*nodes[0].image.argv,
            argv(&["bash", "-c", &script]).as_slice()
        );
        let mut children: Vec<u32> = nodes[1..].iter().map(|node| node.identity.pid).collect();
        children.sort_unstable();
        expected.sort_unstable();
        assert_eq!(children, expected);
        for node in &nodes {
            assert_eq!(
                node.exit.and_then(|exit| exit.status.as_ref()),
                Some(&ExitStatus::Exited { code: 0 }),
                "{node:?}"
            );
        }
        for node in &nodes[1..] {
            assert_eq!(node.parent, Some(nodes[0].identity));
            assert_eq!(node.ppid, root);
        }
        assert_eq!(observer.fold().coverage(), &uncounted(root));
    }

    /// A root that only execs: the exec is no event, the next look finds the image moved and
    /// reads it, and the tree is still never proven complete.
    #[test]
    fn an_exec_is_found_by_the_next_look_and_the_tree_is_never_complete() {
        let marker = marker("exec");
        let (hold_read, hold_write) = std::io::pipe().unwrap();
        let (mut observer, mut child) = watched(
            "bash",
            &format!("exec cat {marker} - <&3"),
            Some(hold_read.into()),
        );
        let root = child.id();
        assert_eq!(observer.fold().coverage(), &uncounted(root));
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line, "ready\n");
        observer
            .sample(Instant::now())
            .expect("the exec, read while cat runs");
        let process = observer.fold().live(root).expect("the root runs");
        assert_eq!(
            &*observer.fold().image(process).unwrap().argv,
            argv(&["cat", &marker, "-"]).as_slice()
        );
        drop(hold_write);
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait_bounded().unwrap().success());
        std::fs::remove_file(&marker).unwrap();
        assert_eq!(observer.fold().nodes().count(), 1);
        assert_eq!(observer.fold().coverage(), &uncounted(root));
    }

    /// The wait status of a descendant this observer is not the parent of is read all the same:
    /// from its unreaped entry, or from the kernel's exit record once its parent reaped it.
    #[test]
    fn a_descendant_s_signal_death_is_reported_with_its_status() {
        let (hold_read, mut hold_write) = std::io::pipe().unwrap();
        let script = "(read line <&3; kill -KILL $BASHPID) & echo $!\nwait $!\nexit 0";
        let (mut observer, mut child) = watched("bash", script, Some(hold_read.into()));
        let root = child.id();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        let killed: u32 = line.trim().parse().expect("child pid");
        observer
            .sample(Instant::now())
            .expect("the child, read while it waits");
        let killed = observer.fold().live(killed).expect("adopted");
        hold_write.write_all(b"go\n").unwrap();
        let failures = until_exited(&mut observer, root);
        assert!(failures.is_empty(), "{failures:?}");
        assert!(child.wait_bounded().unwrap().success());
        let status = |process: ProcessIdentity| {
            observer
                .fold()
                .nodes()
                .find(|node| node.identity == process)
                .and_then(|node| node.exit.cloned())
                .expect("exit joined")
                .status
        };
        assert_eq!(
            status(killed),
            Some(ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false
            })
        );
        let root = observer.fold().nodes().next().expect("the root").identity;
        assert_eq!(status(root), Some(ExitStatus::Exited { code: 0 }));
    }

    /// The pid named another life after the registration than before it: nothing is adopted,
    /// and the registration is withdrawn from the set.
    #[test]
    fn a_registration_whose_pid_changed_life_is_withdrawn_unadopted() {
        let (child, pid) = held_process();
        let before = record(pid, 40, 1);
        for after in [Some(record(pid, 41, 1)), None] {
            let reads = Scripted::default().record(pid, &[after]);
            let mut observer = scripted(reads, TestFold::default(), &[]);
            assert_eq!(observer.attach(before).unwrap(), None);
            assert!(observer.registered.is_empty());
            assert!(observer.source.tokens.is_empty(), "its token names nothing");
        }
        // The same life before and after: adopted, and its registration stays.
        let reads = Scripted::default().record(pid, &[Some(before)]);
        let mut observer = scripted(reads, TestFold::default(), &[]);
        assert_eq!(observer.attach(before).unwrap(), Some(before));
        assert!(observer.registered.contains_key(&life(pid, 40)));
        assert_eq!(
            observer.source.tokens.values().copied().collect::<Vec<_>>(),
            [life(pid, 40)]
        );
        release(child);
    }

    /// A wake makes the set readable once, reports nothing, and leaves the eventfd quiet.
    #[test]
    fn a_wake_is_taken_once_and_reports_nothing() {
        let mut source = Source::open().expect("the event source");
        let mut wake = File::from(source.wake_handle().expect("the wake handle"));
        let mut registered = HashMap::new();
        let mut reports = Vec::new();
        let mut collect = |source: &mut Source| {
            source
                .collect(Some(Duration::ZERO), &mut registered, &mut reports)
                .expect("the set answers")
        };
        assert_eq!(collect(&mut source), 0);
        wake.write_all(&1_u64.to_ne_bytes())
            .expect("raise the wake");
        wake.write_all(&1_u64.to_ne_bytes())
            .expect("raise the wake");
        assert_eq!(collect(&mut source), 1);
        assert_eq!(collect(&mut source), 0);
        assert!(reports.is_empty());
    }

    /// A process's children are those of all its threads, and a process that is gone has none:
    /// its exit empties the list and never fails it.
    #[test]
    fn a_child_list_names_a_running_child_and_is_empty_once_its_parent_is_gone() {
        let (child, pid) = held_process();
        assert!(
            children(std::process::id())
                .expect("this process's children")
                .contains(&pid)
        );
        assert_eq!(children(pid).expect("cat's children"), [0_u32; 0]);
        release(child);
        assert_eq!(
            children(pid).expect("a reaped process's children"),
            [0_u32; 0]
        );
    }
}
