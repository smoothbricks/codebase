//! What each observed process of a job is waiting on (07_api.md, "Process-tree observations").
//!
//! A read looks at one life's threads at one moment, fenced to that life exactly like its usage
//! read (`process_usage`): on Linux by the pidfd the observer holds for it, on macOS by its
//! unique id. A process with any thread running or runnable is observed unblocked:
//! [`ProcessBlockedOn::None`]. Otherwise each wait the kernel names a class for is evidence of
//! it, and the process waits on the one class all that evidence agrees on. A wait the kernel
//! names no class for -- a futex, a poll over many descriptors, a timer -- is no evidence, and
//! evidence that disagrees is none either: the read then has no answer, which is never `none`.
//!
//! What each kernel shows, measured:
//!
//! | class   | Linux                                         | macOS                                      |
//! |---------|-----------------------------------------------|--------------------------------------------|
//! | `none`  | a thread in state `R`                         | a thread in `TH_STATE_RUNNING`             |
//! | `disk`  | a thread in state `D` ("disk sleep")          | a thread in `TH_STATE_UNINTERRUPTIBLE`     |
//! | `stdin` | a read of descriptor 0                        | a reader asleep on descriptor 0's pipe     |
//! | `pipe`  | a read or write of a FIFO descriptor          | a reader asleep on another pipe descriptor |
//! | `socket`| a socket call, or a read or write of a socket | no evidence                                |
//! | `child` | `wait4`/`waitid`                              | no evidence                                |
//! | `lock`  | `flock`, `fcntl(F_SETLKW)`; holder: `/proc/locks` | no evidence                            |
//!
//! The Linux evidence is the call each sleeping thread is in (`/proc/<pid>/task/<tid>/syscall`)
//! and what its descriptor names. macOS shows no unprivileged reader the call a thread sleeps in
//! (`kinfo_proc`'s `p_wchan`/`p_wmesg` read zero; a thread's info carries only its run state),
//! so there the evidence is the kernel's own record of a sleeping reader on a pipe, which only
//! names the sleeper when this process alone holds that end; see the macOS reader.
//!
//! A lock's holder is named by pid only. Which cowshed job owns that pid is the supervisor's
//! knowledge, not the kernel's: [`ReadBlocker::attribute`] is the one way from a read to a
//! record's blocker, and takes that lookup.

use std::io;

use crate::api::dto::JobId;
use crate::api::process::{LockHolder, ProcessBlockedOn};
use crate::runtime::process_tree::ProcessIdentity;

/// A blocker as the kernel showed it, a lock's holder named by pid alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBlocker(ProcessBlockedOn);

impl ReadBlocker {
    /// The record's blocker: a lock holder's job is what `job_of` -- the supervisor's answer to
    /// which of its jobs owns a pid -- says, and absent where no job of its owns the holder.
    pub fn attribute(self, job_of: impl FnOnce(u32) -> Option<JobId>) -> ProcessBlockedOn {
        match self.0 {
            ProcessBlockedOn::Lock { path, holder } => ProcessBlockedOn::Lock {
                path,
                holder: holder.map(|holder| LockHolder {
                    pid: holder.pid,
                    job: job_of(holder.pid),
                }),
            },
            blocked @ (ProcessBlockedOn::None
            | ProcessBlockedOn::Socket
            | ProcessBlockedOn::Pipe
            | ProcessBlockedOn::Child
            | ProcessBlockedOn::Stdin
            | ProcessBlockedOn::Disk) => blocked,
        }
    }
}

/// What reading one life's blocker found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockerSampled {
    /// What it was found waiting on, `none` included; `None` when the kernel showed no evidence
    /// either way.
    Read(Option<ReadBlocker>),
    /// It no longer runs: it exited, reaped or not, or its pid names another life.
    Gone,
}

/// Why a life's blocker could not be read: the kernel call and its error.
#[derive(Debug, thiserror::Error)]
#[error("reading what process {pid} waits on failed at {call}: {source}")]
pub struct BlockerReadError {
    pub pid: u32,
    pub call: &'static str,
    #[source]
    pub source: io::Error,
}

fn failed(pid: u32, call: &'static str) -> impl FnOnce(io::Error) -> BlockerReadError {
    move |source| BlockerReadError { pid, call, source }
}

/// What one thread or descriptor showed.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Evidence {
    Running,
    /// A wait of one class; never [`ProcessBlockedOn::None`], which only a running thread shows.
    Waits(ProcessBlockedOn),
}

/// The process's blocker from all its evidence: `none` when anything runs, else the one class
/// every wait agrees on, else nothing.
fn judge(evidence: impl IntoIterator<Item = Evidence>) -> Option<ProcessBlockedOn> {
    let mut agreed: Option<ProcessBlockedOn> = None;
    let mut disagree = false;
    for evidence in evidence {
        match evidence {
            Evidence::Running => return Some(ProcessBlockedOn::None),
            Evidence::Waits(blocked) => match &agreed {
                None => agreed = Some(blocked),
                Some(first) => disagree |= *first != blocked,
            },
        }
    }
    agreed.filter(|_| !disagree)
}

/// Read what `process` waits on now. `held` is the pidfd the observer opened for this very life.
#[cfg(target_os = "linux")]
pub fn sample(
    process: ProcessIdentity,
    held: std::os::fd::BorrowedFd<'_>,
) -> Result<BlockerSampled, BlockerReadError> {
    let Some(evidence) = linux::evidence(process.pid)? else {
        return Ok(BlockerSampled::Gone);
    };
    // The pidfd names the life it was opened for alone. Unreaped after the read, that life held
    // the pid throughout, so what was read was its own.
    let unreaped = crate::runtime::process_usage::unreaped(held)
        .map_err(failed(process.pid, "pidfd_send_signal"))?;
    if !unreaped {
        return Ok(BlockerSampled::Gone);
    }
    Ok(BlockerSampled::Read(judge(evidence).map(ReadBlocker)))
}

/// Read what `process` waits on now. The read names the pid alone; it is of `process` if the
/// pid still names `process`'s unique id after it, since a life holds its pid until it is
/// reaped. An exited life names no unique id: it is gone, and waits on nothing.
#[cfg(target_os = "macos")]
pub fn sample(process: ProcessIdentity) -> Result<BlockerSampled, BlockerReadError> {
    let pid = libc::pid_t::try_from(process.pid).map_err(|error| {
        failed(process.pid, "pid_t::try_from")(io::Error::new(io::ErrorKind::InvalidInput, error))
    })?;
    let evidence =
        macos::evidence(pid).map_err(|(call, source)| failed(process.pid, call)(source))?;
    let Some(evidence) = evidence else {
        return Ok(BlockerSampled::Gone);
    };
    let record = crate::runtime::job_groups::process_record(pid).map_err(failed(
        process.pid,
        "proc_pidinfo(PROC_PIDT_BSDINFOWITHUNIQID)",
    ))?;
    match record {
        Some(record) if record.unique_id == process.birth.0 => {}
        Some(_) | None => return Ok(BlockerSampled::Gone),
    }
    Ok(BlockerSampled::Read(judge(evidence).map(ReadBlocker)))
}

// ---------------------------------------------------------------------------------------------
// Linux.

#[cfg(target_os = "linux")]
mod linux {
    use std::io::{self, Read as _};
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    use super::{BlockerReadError, Evidence, failed};
    use crate::api::process::{LockHolder, ProcessBlockedOn};

    /// The call a sleeping thread is in, as far as its class goes.
    enum Call {
        Reads(u32),
        Writes(u32),
        Socket,
        Child,
        Locks(u32),
    }

    /// Every task's evidence, `None` when no task of `pid` is alive: it exited or was reaped.
    ///
    /// A task in state `R` runs; one in `D`, the kernel's "disk sleep", waits on disk. One in
    /// `S` waits in the call `/proc/<pid>/task/<tid>/syscall` names (`running` if it woke since),
    /// by the native syscall table -- a process whose executable is not of this word size issues
    /// another table's numbers, which name nothing here. That file needs ptrace-attach access,
    /// which Yama's `ptrace_scope` 1 grants an ancestor; a refusal is an error, never a guess.
    pub(super) fn evidence(pid: u32) -> Result<Option<Vec<Evidence>>, BlockerReadError> {
        let tasks = match std::fs::read_dir(format!("/proc/{pid}/task")) {
            Ok(tasks) => tasks,
            Err(error) if gone(&error) => {
                // An unmounted procfs proves nothing about `pid`.
                std::fs::metadata("/proc/self/stat").map_err(failed(pid, "/proc/self/stat"))?;
                return Ok(None);
            }
            Err(error) => return Err(failed(pid, "/proc/<pid>/task")(error)),
        };
        let mut alive = false;
        let mut native = None;
        let mut evidence = Vec::new();
        for task in tasks {
            let task = task.map_err(failed(pid, "/proc/<pid>/task"))?;
            let tid = task.file_name();
            let tid = tid.to_str().ok_or_else(|| {
                failed(pid, "/proc/<pid>/task")(invalid(format!("a task named {tid:?}")))
            })?;
            match task_evidence(pid, tid, &mut native)? {
                Task::Gone => {}
                Task::Alive(shown) => {
                    alive = true;
                    evidence.extend(shown);
                }
            }
        }
        Ok(alive.then_some(evidence))
    }

    /// What one task showed.
    enum Task {
        /// Exited, or gone since the listing; the process's other tasks may live on.
        Gone,
        /// Alive, and running or waiting on what its evidence names, if anything.
        Alive(Option<Evidence>),
    }

    /// `native` is whether the process issues the native syscall table, asked once per read.
    fn task_evidence(
        pid: u32,
        tid: &str,
        native: &mut Option<bool>,
    ) -> Result<Task, BlockerReadError> {
        const STAT: &str = "/proc/<pid>/task/<tid>/stat";
        const SYSCALL: &str = "/proc/<pid>/task/<tid>/syscall";
        let Some(state) = task_state(pid, tid).map_err(failed(pid, STAT))? else {
            return Ok(Task::Gone);
        };
        let shown = match state {
            'Z' | 'X' | 'x' => return Ok(Task::Gone),
            'R' => Some(Evidence::Running),
            'D' => Some(Evidence::Waits(ProcessBlockedOn::Disk)),
            'S' => {
                let syscall =
                    match std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/syscall")) {
                        Ok(syscall) => syscall,
                        // A task whose stat still reads has a syscall file wherever the kernel
                        // provides one.
                        Err(error) if gone(&error) => {
                            return match task_state(pid, tid).map_err(failed(pid, STAT))? {
                                Some(_) => Err(failed(pid, SYSCALL)(error)),
                                None => Ok(Task::Gone),
                            };
                        }
                        Err(error) => return Err(failed(pid, SYSCALL)(error)),
                    };
                match sleeping_in(&syscall).map_err(failed(pid, SYSCALL))? {
                    Sleep::Woke => Some(Evidence::Running),
                    Sleep::Outside => None,
                    Sleep::In(nr, args) => match call(nr, args) {
                        Some(call) if native_table_once(pid, native)? => {
                            class(pid, call)?.map(Evidence::Waits)
                        }
                        _ => None,
                    },
                }
            }
            // Stopped, traced, parked or idle: waiting on nothing a class names.
            _ => None,
        };
        Ok(Task::Alive(shown))
    }

    /// [`native_table`], asked at most once per read.
    fn native_table_once(pid: u32, native: &mut Option<bool>) -> Result<bool, BlockerReadError> {
        if let Some(known) = *native {
            return Ok(known);
        }
        let known = native_table(pid).map_err(failed(pid, "/proc/<pid>/exe"))?;
        *native = Some(known);
        Ok(known)
    }

    fn gone(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
    }

    fn invalid(what: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, what)
    }

    /// A task's state, field 3 of its stat, after the parenthesized command name that may hold
    /// anything; `None` once the task is gone.
    fn task_state(pid: u32, tid: &str) -> io::Result<Option<char>> {
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/stat")) {
            Ok(stat) => stat,
            Err(error) if gone(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        stat.rfind(')')
            .and_then(|closing| stat[closing + 1..].split_whitespace().next())
            .and_then(|state| state.chars().next())
            .map(Some)
            .ok_or_else(|| invalid(format!("task {tid}'s stat {stat:?} has no state")))
    }

    /// What `/proc/<pid>/task/<tid>/syscall` says a sleeping task is doing.
    enum Sleep {
        /// `running`: it ran again before the kernel could read it asleep.
        Woke,
        /// Asleep outside any system call (`-1`).
        Outside,
        /// In system call `nr` with these arguments.
        In(i64, [u64; 6]),
    }

    fn sleeping_in(syscall: &str) -> io::Result<Sleep> {
        let syscall = syscall.trim_end();
        if syscall == "running" {
            return Ok(Sleep::Woke);
        }
        let malformed = || invalid(format!("the syscall line {syscall:?}"));
        let mut words = syscall.split(' ');
        let nr: i64 = words
            .next()
            .and_then(|nr| nr.parse().ok())
            .ok_or_else(malformed)?;
        if nr < 0 {
            return Ok(Sleep::Outside);
        }
        let mut args = [0_u64; 6];
        for arg in &mut args {
            *arg = words
                .next()
                .and_then(|word| word.strip_prefix("0x"))
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                .ok_or_else(malformed)?;
        }
        Ok(Sleep::In(nr, args))
    }

    /// Whether `pid` runs an executable of this build's word size, whose system calls are the
    /// native table's: `EI_CLASS` of the ELF identification `/proc/<pid>/exe` starts with.
    fn native_table(pid: u32) -> io::Result<bool> {
        #[cfg(target_pointer_width = "64")]
        const NATIVE: u8 = 2; // ELFCLASS64
        #[cfg(target_pointer_width = "32")]
        const NATIVE: u8 = 1; // ELFCLASS32
        let mut ident = [0_u8; 5];
        std::fs::File::open(format!("/proc/{pid}/exe"))?.read_exact(&mut ident)?;
        if ident[..4] != *b"\x7fELF" {
            return Err(invalid(format!(
                "process {pid}'s executable starts {ident:?}, not an ELF identification"
            )));
        }
        Ok(ident[4] == NATIVE)
    }

    /// The class-naming calls a thread can sleep in; `None` for every other.
    fn call(nr: i64, args: [u64; 6]) -> Option<Call> {
        let fd = || u32::try_from(args[0]).ok();
        match nr {
            libc::SYS_read
            | libc::SYS_readv
            | libc::SYS_pread64
            | libc::SYS_preadv
            | libc::SYS_preadv2 => fd().map(Call::Reads),
            libc::SYS_write
            | libc::SYS_writev
            | libc::SYS_pwrite64
            | libc::SYS_pwritev
            | libc::SYS_pwritev2 => fd().map(Call::Writes),
            libc::SYS_accept
            | libc::SYS_accept4
            | libc::SYS_connect
            | libc::SYS_recvfrom
            | libc::SYS_recvmsg
            | libc::SYS_recvmmsg
            | libc::SYS_sendto
            | libc::SYS_sendmsg
            | libc::SYS_sendmmsg => Some(Call::Socket),
            libc::SYS_wait4 | libc::SYS_waitid => Some(Call::Child),
            libc::SYS_flock => fd().map(Call::Locks),
            libc::SYS_fcntl => match i32::try_from(args[1]) {
                Ok(libc::F_SETLKW | libc::F_OFD_SETLKW) => fd().map(Call::Locks),
                _ => None,
            },
            _ => None,
        }
    }

    /// The class `call` names. A descriptor closed since the call began names none.
    fn class(pid: u32, call: Call) -> Result<Option<ProcessBlockedOn>, BlockerReadError> {
        const DESCRIPTOR: &str = "/proc/<pid>/fd/<fd>";
        const LINK: &str = "readlink /proc/<pid>/fd/<fd>";
        let descriptor = |fd: u32| -> Result<Option<std::fs::Metadata>, BlockerReadError> {
            match std::fs::metadata(format!("/proc/{pid}/fd/{fd}")) {
                Ok(target) => Ok(Some(target)),
                Err(error) if gone(&error) => Ok(None),
                Err(error) => Err(failed(pid, DESCRIPTOR)(error)),
            }
        };
        let of_type = |target: std::fs::Metadata| {
            let kind = target.file_type();
            if kind.is_fifo() {
                Some(ProcessBlockedOn::Pipe)
            } else if kind.is_socket() {
                Some(ProcessBlockedOn::Socket)
            } else {
                None
            }
        };
        Ok(match call {
            Call::Reads(0) => Some(ProcessBlockedOn::Stdin),
            Call::Reads(fd) | Call::Writes(fd) => descriptor(fd)?.and_then(of_type),
            Call::Socket => Some(ProcessBlockedOn::Socket),
            Call::Child => Some(ProcessBlockedOn::Child),
            Call::Locks(fd) => {
                let Some(target) = descriptor(fd)? else {
                    return Ok(None);
                };
                let path = match std::fs::read_link(format!("/proc/{pid}/fd/{fd}")) {
                    Ok(path) => path,
                    Err(error) if gone(&error) => return Ok(None),
                    Err(error) => return Err(failed(pid, LINK)(error)),
                };
                let path = path.into_os_string().into_string().map_err(|path| {
                    failed(pid, LINK)(invalid(format!("the locked path {path:?} is not UTF-8")))
                })?;
                let holder = lock_holder(pid, target.dev(), target.ino())
                    .map_err(failed(pid, "/proc/locks"))?;
                Some(ProcessBlockedOn::Lock {
                    path,
                    holder: holder.map(|pid| LockHolder { pid, job: None }),
                })
            }
        })
    }

    /// The pid holding the lock `waiter` waits for on the file `dev`/`ino`, from `/proc/locks`.
    ///
    /// Each lock the kernel holds is one numbered line; the requests blocked on it follow under
    /// the same number, marked `->` (deeper by one space per level when a request waits on
    /// another request). Every field before the pid is one word: kind, mode, type, then the pid
    /// and `major:minor:inode` in hex, hex and decimal (fs/locks.c `lock_get_status`). The holder
    /// is the numbered lock's pid; an open-file-description lock shows `-1`, no process, and a
    /// lock whose holder this pid namespace cannot see is not listed at all. `None` when no
    /// request of `waiter` on that file is listed.
    fn lock_holder(waiter: u32, dev: u64, ino: u64) -> io::Result<Option<u32>> {
        let locks = std::fs::read_to_string("/proc/locks")?;
        let file = (libc::major(dev), libc::minor(dev), ino);
        let mut held: Option<(&str, i64)> = None;
        for line in locks.lines() {
            let malformed = || invalid(format!("the /proc/locks line {line:?}"));
            let mut words = line.split_whitespace();
            let number = words.next().ok_or_else(malformed)?;
            let mut kind = words.next().ok_or_else(malformed)?;
            let request = kind == "->";
            if request {
                kind = words.next().ok_or_else(malformed)?;
            }
            let pid: i64 = words
                .nth(2)
                .and_then(|pid| pid.parse().ok())
                .ok_or_else(malformed)?;
            if !request {
                held = Some((number, pid));
                continue;
            }
            if pid != i64::from(waiter) || on_file(words.next()) != Some(file) {
                continue;
            }
            return match held {
                Some((held_number, holder)) if held_number == number => {
                    Ok(u32::try_from(holder).ok())
                }
                _ => Err(invalid(format!(
                    "the {kind} request {line:?} follows no lock"
                ))),
            };
        }
        Ok(None)
    }

    /// `major:minor:inode` as `/proc/locks` writes it; `None` for a lock on no inode.
    fn on_file(word: Option<&str>) -> Option<(u32, u32, u64)> {
        let mut parts = word?.split(':');
        let major = u32::from_str_radix(parts.next()?, 16).ok()?;
        let minor = u32::from_str_radix(parts.next()?, 16).ok()?;
        let inode = parts.next()?.parse().ok()?;
        Some((major, minor, inode))
    }
}

// ---------------------------------------------------------------------------------------------
// macOS.

/// `PROC_PIDFDPIPEINFO` of `<sys/proc_info.h>`.
#[cfg(target_os = "macos")]
pub(crate) const PROC_PIDFDPIPEINFO: libc::c_int = 6;

/// `struct pipe_fdinfo` of `<sys/proc_info.h>` (its `proc_fileinfo` laid out in place), which the
/// libc crate does not declare.
#[cfg(target_os = "macos")]
#[repr(C)]
pub(crate) struct PipeFdInfo {
    pub(crate) fi_openflags: u32,
    pub(crate) fi_status: u32,
    pub(crate) fi_offset: libc::off_t,
    pub(crate) fi_type: i32,
    pub(crate) fi_guardflags: u32,
    pub(crate) pipe_stat: libc::vinfo_stat,
    pub(crate) pipe_handle: u64,
    pub(crate) pipe_peerhandle: u64,
    pub(crate) pipe_status: libc::c_int,
    pub(crate) rfu_1: libc::c_int,
}

#[cfg(target_os = "macos")]
mod macos {
    use std::io;

    use super::{Evidence, PROC_PIDFDPIPEINFO, PipeFdInfo};
    use crate::api::process::ProcessBlockedOn;

    /// `PROC_PIDLISTTHREADS` of `<sys/proc_info.h>`: the task's thread handles.
    const PROC_PIDLISTTHREADS: libc::c_int = 6;
    /// `PROC_FP_SHARED` of `<sys/proc_info.h>`: the open file is referenced more than once.
    const PROC_FP_SHARED: u32 = 1;
    /// `PIPE_WANTR` of xnu `<sys/pipe.h>`: a reader sleeps on this end for data.
    const PIPE_WANTR: libc::c_int = 0x8;

    type Failed = (&'static str, io::Error);

    /// Every thread's and pipe's evidence, `None` once the process is gone.
    ///
    /// A thread's info carries its run state alone: `TH_STATE_RUNNING` (running or runnable),
    /// `TH_STATE_UNINTERRUPTIBLE` (the wait `ps` names disk), or another wait whose call no
    /// unprivileged read names -- measured on Darwin 25.6: `kinfo_proc`'s `p_wchan`, `p_wmesg`
    /// and `e_wmesg` are zero for a process asleep in `read`, `accept`, `wait4` and `flock`.
    ///
    /// A pipe is evidence of the one wait the kernel records per end: `pipe_read` sets
    /// `PIPE_WANTR` on the end before it sleeps and a write clears it on waking it, and the end's
    /// info reports that state. It names this process's thread only where this descriptor is the
    /// only reference to that end (`PROC_FP_SHARED` clear); a shared end is no evidence. A read
    /// a signal interrupted leaves the flag set until the next write: while the process runs,
    /// its running thread answers first.
    ///
    /// Measured with no evidence: a socket's buffer records no sleeper (`sbi_flags` bit 0x4 is
    /// `SB_RECV`, set on every receive buffer, sleepers are counted where no info reports them),
    /// `accept` sleeps on the listening socket with no recorded state, and `wait4` and a lock
    /// wait (`flock` or `F_SETLKW`) leave nothing on any descriptor; `F_GETLK` reports a `flock`
    /// holder as pid -1. Those classes are absent here, never guessed.
    pub(super) fn evidence(pid: libc::pid_t) -> Result<Option<Vec<Evidence>>, Failed> {
        let Some(threads) = threads(pid)? else {
            return Ok(None);
        };
        let mut evidence = Vec::new();
        for thread in threads {
            let Some(state) = run_state(pid, thread)? else {
                continue;
            };
            match state {
                libc::TH_STATE_RUNNING => evidence.push(Evidence::Running),
                libc::TH_STATE_UNINTERRUPTIBLE => {
                    evidence.push(Evidence::Waits(ProcessBlockedOn::Disk));
                }
                _ => {}
            }
        }
        let Some(descriptors) = descriptors(pid)? else {
            return Ok(None);
        };
        for descriptor in descriptors {
            if i32::try_from(descriptor.proc_fdtype) != Ok(libc::PROX_FDTYPE_PIPE) {
                continue;
            }
            let Some(pipe) = pipe(pid, descriptor.proc_fd)? else {
                continue;
            };
            if pipe.fi_status & PROC_FP_SHARED == 0 && pipe.pipe_status & PIPE_WANTR != 0 {
                evidence.push(Evidence::Waits(if descriptor.proc_fd == 0 {
                    ProcessBlockedOn::Stdin
                } else {
                    ProcessBlockedOn::Pipe
                }));
            }
        }
        Ok(Some(evidence))
    }

    fn bytes(size: usize) -> Result<libc::c_int, Failed> {
        libc::c_int::try_from(size).map_err(|error| {
            (
                "proc_pidinfo buffer size",
                io::Error::new(io::ErrorKind::InvalidInput, error),
            )
        })
    }

    /// A `proc_pidinfo`/`proc_pidfdinfo` that wrote `written` bytes where `size` were asked: `Ok`
    /// when its errno is one of `gone`, which the caller reads as nothing left to read.
    fn refused(
        call: &'static str,
        written: libc::c_int,
        size: libc::c_int,
        gone: &[libc::c_int],
    ) -> Result<(), Failed> {
        if written > 0 {
            return Err((
                call,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("returned {written} bytes, expected {size}"),
                ),
            ));
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(errno) if gone.contains(&errno) => Ok(()),
            _ => Err((call, error)),
        }
    }

    /// The whole `flavor` listing of `pid`, `None` once it is gone. A buffer the answer fills
    /// may have cut it short, so the buffer grows until the answer leaves room.
    fn listing<T: Copy>(
        pid: libc::pid_t,
        flavor: libc::c_int,
        call: &'static str,
        empty: T,
    ) -> Result<Option<Vec<T>>, Failed> {
        let mut capacity = 64;
        loop {
            let mut listed = vec![empty; capacity];
            let size = bytes(capacity * size_of::<T>())?;
            // SAFETY: `listed` provides `size` writable bytes of plain C records.
            let written =
                unsafe { libc::proc_pidinfo(pid, flavor, 0, listed.as_mut_ptr().cast(), size) };
            if written <= 0 {
                refused(call, written, size, &[libc::ESRCH])?;
                return Ok(None);
            }
            if written == size {
                capacity *= 2;
                continue;
            }
            let written = usize::try_from(written)
                .map_err(|error| (call, io::Error::new(io::ErrorKind::InvalidData, error)))?;
            listed.truncate(written / size_of::<T>());
            return Ok(Some(listed));
        }
    }

    /// The task's thread handles, `None` once it is gone.
    fn threads(pid: libc::pid_t) -> Result<Option<Vec<u64>>, Failed> {
        listing(
            pid,
            PROC_PIDLISTTHREADS,
            "proc_pidinfo(PROC_PIDLISTTHREADS)",
            0_u64,
        )
    }

    /// The process's descriptors, `None` once it is gone.
    fn descriptors(pid: libc::pid_t) -> Result<Option<Vec<libc::proc_fdinfo>>, Failed> {
        let empty = libc::proc_fdinfo {
            proc_fd: 0,
            proc_fdtype: 0,
        };
        listing(
            pid,
            libc::PROC_PIDLISTFDS,
            "proc_pidinfo(PROC_PIDLISTFDS)",
            empty,
        )
    }

    /// A thread's run state, `None` once the thread is gone.
    fn run_state(pid: libc::pid_t, thread: u64) -> Result<Option<libc::c_int>, Failed> {
        let mut info = std::mem::MaybeUninit::<libc::proc_threadinfo>::zeroed();
        let size = bytes(size_of::<libc::proc_threadinfo>())?;
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTHREADINFO,
                thread,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if written != size {
            refused(
                "proc_pidinfo(PROC_PIDTHREADINFO)",
                written,
                size,
                &[libc::ESRCH],
            )?;
            return Ok(None);
        }
        // SAFETY: proc_pidinfo filled all `size` bytes.
        Ok(Some(unsafe { info.assume_init() }.pth_run_state))
    }

    /// A pipe descriptor's info, `None` once it is closed (or the process is gone, which the
    /// fence finds).
    fn pipe(pid: libc::pid_t, fd: i32) -> Result<Option<PipeFdInfo>, Failed> {
        let mut info = std::mem::MaybeUninit::<PipeFdInfo>::zeroed();
        let size = bytes(size_of::<PipeFdInfo>())?;
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidfdinfo(pid, fd, PROC_PIDFDPIPEINFO, info.as_mut_ptr().cast(), size)
        };
        if written != size {
            refused(
                "proc_pidfdinfo(PROC_PIDFDPIPEINFO)",
                written,
                size,
                &[libc::EBADF, libc::ESRCH],
            )?;
            return Ok(None);
        }
        // SAFETY: proc_pidfdinfo filled all `size` bytes.
        Ok(Some(unsafe { info.assume_init() }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read as _, Write as _};
    use std::os::fd::AsRawFd as _;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use super::{BlockerSampled, Evidence, ReadBlocker, judge, sample};
    use crate::api::dto::{CommandArg, JobId, UtcTimestamp};
    use crate::api::process::{
        BlockerChange, JobProcessDelta, JobProcessEvent, JobProcessSample, LockHolder,
        ProcessBlockedOn,
    };
    use crate::fork_lock::Spawn as _;
    use crate::runtime::process_stream::{JobProcessFold, JobProcessObservation};
    use crate::runtime::process_tree::{ProcessImage, ProcessObservation};
    use crate::runtime::process_usage::tests::{Observed, hold_byte, observed};

    const ROLE: &str = "COWSHED_PROCESS_BLOCKER_ROLE";
    const ROLE_TEST: &str = "runtime::process_blocker::tests::process_blocker_role";
    const MARK: &str = "process-blocker-fixture";

    #[test]
    fn a_running_thread_answers_none_and_disagreeing_waits_answer_nothing() {
        use ProcessBlockedOn::{Pipe, Stdin};
        let waits = Evidence::Waits;
        assert_eq!(
            judge([waits(Pipe), Evidence::Running, waits(Stdin)]),
            Some(ProcessBlockedOn::None),
            "a process whose thread runs is not blocked, whatever another thread waits on"
        );
        assert_eq!(judge([waits(Pipe), waits(Pipe)]), Some(Pipe));
        assert_eq!(judge([waits(Pipe), waits(Stdin)]), None, "no one answer");
        assert_eq!(
            judge([]),
            None,
            "waits that name no class are no evidence, never `none`"
        );
    }

    #[test]
    fn only_the_supervisors_lookup_names_a_lock_holders_job() {
        let lock = |job| ProcessBlockedOn::Lock {
            path: "/ws/.git/index.lock".to_owned(),
            holder: Some(LockHolder { pid: 7, job }),
        };
        let read = ReadBlocker(lock(None));
        let second = JobId::new(2).expect("a job id");
        assert_eq!(
            read.clone().attribute(|pid| (pid == 7).then_some(second)),
            lock(Some(second))
        );
        assert_eq!(
            read.attribute(|_| None),
            lock(None),
            "no job of ours holds it"
        );
        assert_eq!(
            ReadBlocker(ProcessBlockedOn::Pipe).attribute(|pid| panic!("no holder, yet {pid}")),
            ProcessBlockedOn::Pipe
        );
    }

    // -----------------------------------------------------------------------------------------
    // Members blocked on each class, read from the kernel and folded.

    /// The fixture roles, run only when a test re-executes this binary with [`ROLE`] set.
    #[test]
    fn process_blocker_role() {
        let Some(role) = std::env::var_os(ROLE) else {
            return;
        };
        let role = role.into_string().expect("a UTF-8 role");
        let (role, argument) = role.split_once(' ').unwrap_or((role.as_str(), ""));
        match role {
            "busy" => {
                report("busy");
                spin_until_byte();
            }
            "stdin" => {
                report("waiting");
                assert!(hold_byte(), "the release byte");
                report("busy");
                spin_until_byte();
            }
            "pipe" => {
                let (mut read, _write) = std::io::pipe().expect("a pipe");
                report("waiting");
                let got = read.read(&mut [0_u8]);
                panic!("a pipe whose writer this process holds answered {got:?}");
            }
            "accept" => {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listen");
                report("waiting");
                let accepted = listener.accept();
                panic!("nothing connects, yet accept answered {accepted:?}");
            }
            "child" => {
                // Held by the pipe this process holds open: it exits when this one does.
                let mut held = Command::new(std::env::current_exe().expect("test binary"))
                    .args(["--exact", ROLE_TEST, "--nocapture"])
                    .env(ROLE, "hold")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .spawn_locked()
                    .expect("spawn the held child");
                report("waiting");
                let status = held.wait();
                panic!("the held child ended first: {status:?}");
            }
            "hold" => while hold_byte() {},
            "lock" => {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(argument)
                    .expect("open the lock file");
                report("waiting");
                // SAFETY: flock on a descriptor this role owns.
                while unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                    let error = std::io::Error::last_os_error();
                    assert_eq!(
                        error.kind(),
                        std::io::ErrorKind::Interrupted,
                        "flock: {error}"
                    );
                }
                report("locked");
                spin_until_byte();
            }
            other => panic!("unknown role {other:?}"),
        }
    }

    fn report(what: &str) {
        let mut out = std::io::stdout();
        writeln!(out, "{MARK} {what} {}", std::process::id()).expect("report");
        out.flush().expect("report");
    }

    /// Run on this CPU without sleeping until stdin is readable, then take its byte.
    fn spin_until_byte() {
        let mut stdin = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        let mut spin = 0_u64;
        loop {
            // SAFETY: one pollfd, polled without waiting.
            let ready = unsafe { libc::poll(&mut stdin, 1, 0) };
            assert!(ready >= 0, "poll: {}", std::io::Error::last_os_error());
            if ready > 0 {
                hold_byte();
                return;
            }
            for _ in 0..10_000 {
                spin = std::hint::black_box(spin.wrapping_mul(31).wrapping_add(7));
            }
        }
    }

    /// A re-executed fixture role, held by its stdin and reporting on its stdout; killed and
    /// reaped when dropped.
    struct Fixture {
        child: std::process::Child,
        release: std::process::ChildStdin,
        lines: std::io::Lines<BufReader<std::process::ChildStdout>>,
        life: Observed,
    }

    impl Fixture {
        /// Start `role` and wait for its report `ready`.
        fn start(role: &str, ready: &str) -> Self {
            let mut child = Command::new(std::env::current_exe().expect("test binary"))
                .args(["--exact", ROLE_TEST, "--nocapture"])
                .env(ROLE, role)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn_locked()
                .expect("spawn a fixture");
            let release = child.stdin.take().expect("stdin");
            let lines = BufReader::new(child.stdout.take().expect("stdout")).lines();
            let life = observed(child.id());
            let mut fixture = Self {
                child,
                release,
                lines,
                life,
            };
            fixture.expect(ready);
            fixture
        }

        /// Wait for the report `what`.
        fn expect(&mut self, what: &str) {
            for line in self.lines.by_ref() {
                let line = line.expect("fixture stdout");
                let mut words = line.split_whitespace();
                if words.next() == Some(MARK) && words.next() == Some(what) {
                    let pid: u32 = words.next().expect("a pid").parse().expect("a pid");
                    assert_eq!(pid, self.life.identity.pid, "the fixture reports itself");
                    return;
                }
            }
            panic!("the fixture ended before reporting {what}");
        }

        fn release(&mut self) {
            self.release.write_all(b"x").expect("release");
        }

        /// One read of the life, its lock holder's job named by `job_of`.
        fn read(&self, job_of: impl FnOnce(u32) -> Option<JobId>) -> Option<ProcessBlockedOn> {
            #[cfg(target_os = "linux")]
            let sampled = {
                use std::os::fd::AsFd as _;
                sample(self.life.identity, self.life.pidfd.as_fd())
            };
            #[cfg(target_os = "macos")]
            let sampled = sample(self.life.identity);
            match sampled.expect("a blocker read") {
                BlockerSampled::Read(read) => read.map(|read| read.attribute(job_of)),
                BlockerSampled::Gone => panic!("the fixture is held alive"),
            }
        }

        /// Read the life until it shows `expected`, the state its last report said it was
        /// entering, and return that read. Each read is the kernel's answer at that moment; one
        /// taken before the fixture reached the call it reported shows it running. A read that
        /// has not shown `expected` ten seconds on is a wrong answer.
        fn shows(
            &self,
            expected: Option<ProcessBlockedOn>,
            job_of: impl Fn(u32) -> Option<JobId>,
        ) -> Option<ProcessBlockedOn> {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let read = self.read(&job_of);
                if read == expected {
                    return read;
                }
                assert!(
                    Instant::now() < deadline,
                    "the member was read as {read:?}, expected {expected:?}"
                );
                std::thread::yield_now();
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.child.kill().expect("kill the fixture");
            self.child.wait().expect("reap the fixture");
        }
    }

    fn unowned(_: u32) -> Option<JobId> {
        None
    }

    /// The real job fold over one fixture: the blocker reads it is given, the events it emitted,
    /// and the record those events replay to.
    struct Folded {
        fold: JobProcessFold,
        events: Vec<JobProcessEvent>,
    }

    impl Folded {
        fn of(fixture: &Fixture) -> Self {
            let mut folded = Self {
                fold: JobProcessFold::new(JobId::new(1).expect("a job id"), Instant::now()),
                events: Vec::new(),
            };
            folded.apply(JobProcessObservation::Tree(ProcessObservation::Root {
                process: fixture.life.identity,
                ppid: std::process::id(),
                at: utc(),
                image: ProcessImage {
                    program: "fixture".to_owned(),
                    argv: vec![CommandArg::new("fixture")],
                },
            }));
            folded
        }

        fn apply(&mut self, observation: JobProcessObservation) {
            self.fold
                .apply(observation, &mut self.events)
                .expect("a consistent observation");
        }

        fn blocked(&mut self, fixture: &Fixture, blocked_on: Option<ProcessBlockedOn>) {
            self.apply(JobProcessObservation::Blocker {
                process: fixture.life.identity,
                blocked_on,
            });
        }

        /// Every blocker change emitted, in order.
        fn changes(&self) -> Vec<BlockerChange> {
            self.events
                .iter()
                .filter_map(|event| match event {
                    JobProcessEvent::Changed(JobProcessDelta::Blocker { blocked_on, .. }) => {
                        Some(blocked_on.clone())
                    }
                    _ => None,
                })
                .collect()
        }

        /// The record a consumer rebuilds from the events alone, which is the fold's own.
        fn replayed(&self) -> JobProcessSample {
            let mut record = None;
            for event in &self.events {
                match event {
                    JobProcessEvent::Born { process, .. } => record = Some(process.clone()),
                    JobProcessEvent::Changed(delta) => {
                        delta.apply(record.as_mut().expect("a change after the birth"));
                    }
                    other => panic!("only a birth and changes were folded: {other:?}"),
                }
            }
            let record = record.expect("the birth");
            assert_eq!(
                std::slice::from_ref(&record),
                self.fold.tree(utc()).processes.as_slice(),
                "the events replay to the fold's record"
            );
            record
        }
    }

    fn utc() -> UtcTimestamp {
        UtcTimestamp::new("2026-10-07T00:00:00Z").expect("a timestamp")
    }

    /// What this platform's kernel shows of a wait this module classes `on_linux`: macOS shows
    /// no evidence of a socket, child or lock wait (see the macOS reader).
    fn on_linux_only(on_linux: ProcessBlockedOn) -> Option<ProcessBlockedOn> {
        cfg!(target_os = "linux").then_some(on_linux)
    }

    #[test]
    fn a_busy_member_is_observed_unblocked() {
        let busy = Fixture::start("busy", "busy");
        let mut folded = Folded::of(&busy);
        let read = busy.shows(Some(ProcessBlockedOn::None), unowned);
        folded.blocked(&busy, read);
        assert_eq!(
            folded.changes(),
            [BlockerChange::Set(ProcessBlockedOn::None)]
        );
        folded.replayed();
    }

    /// Blocked reading stdin, then running once a byte arrives: SET `stdin`, then SET `none`.
    #[test]
    fn a_member_reading_stdin_is_blocked_on_stdin_until_it_runs() {
        let mut reader = Fixture::start("stdin", "waiting");
        let mut folded = Folded::of(&reader);
        let read = reader.shows(Some(ProcessBlockedOn::Stdin), unowned);
        folded.blocked(&reader, read);
        reader.release();
        reader.expect("busy");
        let read = reader.shows(Some(ProcessBlockedOn::None), unowned);
        folded.blocked(&reader, read);
        assert_eq!(
            folded.changes(),
            [
                BlockerChange::Set(ProcessBlockedOn::Stdin),
                BlockerChange::Set(ProcessBlockedOn::None),
            ]
        );
        assert_eq!(folded.replayed().blocked_on, Some(ProcessBlockedOn::None));
    }

    #[test]
    fn a_member_reading_an_empty_pipe_is_blocked_on_the_pipe() {
        let reader = Fixture::start("pipe", "waiting");
        assert_eq!(
            reader.shows(Some(ProcessBlockedOn::Pipe), unowned),
            Some(ProcessBlockedOn::Pipe)
        );
    }

    #[test]
    fn a_member_in_accept_is_blocked_on_its_socket() {
        let server = Fixture::start("accept", "waiting");
        let expected = on_linux_only(ProcessBlockedOn::Socket);
        assert_eq!(server.shows(expected.clone(), unowned), expected);
    }

    #[test]
    fn a_member_waiting_for_its_child_is_blocked_on_the_child() {
        let parent = Fixture::start("child", "waiting");
        let expected = on_linux_only(ProcessBlockedOn::Child);
        assert_eq!(parent.shows(expected.clone(), unowned), expected);
    }

    /// This test holds `flock` on a file and a member blocks on it: the read names `lock`, the
    /// path, and this process as the holder, with the job the supervisor's lookup names for it.
    /// Released, the member runs and the fold SETs `none`, which keeps no path or holder.
    #[test]
    fn a_member_waiting_for_a_held_flock_names_the_lock_its_path_and_holder() {
        // Beside this test binary: a file of the build's own tree, never a shared /tmp.
        let path = std::env::current_exe()
            .expect("test binary")
            .parent()
            .expect("its directory")
            .join(format!("process-blocker-lock-{}", std::process::id()));
        let held = std::fs::File::create(&path).expect("create the lock file");
        // SAFETY: flock on a descriptor this test owns.
        let locked = unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(locked, 0, "flock: {}", std::io::Error::last_os_error());
        let holder = std::process::id();
        let second = JobId::new(2).expect("a job id");
        let second_job_holds = |pid| (pid == holder).then_some(second);

        let mut waiter = Fixture::start(&format!("lock {}", path.display()), "waiting");
        let mut folded = Folded::of(&waiter);
        let lock = ProcessBlockedOn::Lock {
            path: std::fs::canonicalize(&path)
                .expect("the lock file's path")
                .into_os_string()
                .into_string()
                .expect("a UTF-8 path"),
            holder: Some(LockHolder {
                pid: holder,
                job: Some(second),
            }),
        };
        let expected = on_linux_only(lock.clone());
        let read = waiter.shows(expected.clone(), second_job_holds);
        assert_eq!(read, expected);
        folded.blocked(&waiter, read);

        drop(held);
        waiter.expect("locked");
        let read = waiter.shows(Some(ProcessBlockedOn::None), second_job_holds);
        folded.blocked(&waiter, read);
        let mut changes = Vec::from_iter(expected.map(BlockerChange::Set));
        changes.push(BlockerChange::Set(ProcessBlockedOn::None));
        assert_eq!(folded.changes(), changes);
        assert_eq!(
            folded.replayed().blocked_on,
            Some(ProcessBlockedOn::None),
            "released, no path or holder stays"
        );
        std::fs::remove_file(&path).expect("remove the lock file");
    }
}
