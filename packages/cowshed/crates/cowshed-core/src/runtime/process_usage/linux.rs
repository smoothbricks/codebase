//! A life's own counters as Linux keeps them: `/proc/<pid>/stat` and `/proc/<pid>/io`, fenced
//! to the life by the pidfd its observer holds.

use std::io;
use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::time::Instant;

use super::{OwnCounters, SampleError, Sampled, StartStamp, unit};
use crate::api::process::{ProcessIoUnavailable, ProcessStorageIo};
use crate::api::resources::{CpuMicros, ResidentBytes, StorageIoBytes};
use crate::runtime::process_tree::ProcessIdentity;

/// Read `process`'s own counters at the supervisor's sample tick `at`. `held` is the pidfd the
/// observer opened for this very life: procfs names a process by pid alone, so a read is of
/// `process` only if that pidfd still names an unreaped process after it. A read without one is
/// refused, never guessed from the pid. The pidfd is the whole fence, so the start an earlier
/// read anchored fences nothing here and `_start` is unread: a pid reused within one clock tick
/// repeats it, and the fold names a reading whose start differs
/// ([`super::UsageFoldError::OtherLife`]). The caller folds a proven reading once.
pub fn sample(
    process: ProcessIdentity,
    held: Option<BorrowedFd<'_>>,
    _start: Option<StartStamp>,
    at: Instant,
) -> Result<Sampled, SampleError> {
    let failed = |source| SampleError::Read {
        pid: process.pid,
        source,
    };
    let held = held.ok_or_else(|| {
        failed(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no pidfd is held for the life, so no read of its pid is proven its own",
        ))
    })?;
    let pid =
        libc::pid_t::try_from(process.pid).map_err(|error| failed(io::Error::other(error)))?;
    let Some(counters) = own_counters(pid).map_err(failed)? else {
        return Ok(Sampled::Gone);
    };
    if !unreaped(held).map_err(failed)? {
        return Ok(Sampled::Gone);
    }
    Ok(Sampled::Read(counters.reading(at)))
}

/// Whether the process a pidfd names has not been reaped: signal 0 reaches an exited, unreaped
/// process and fails with `ESRCH` only once it is reaped.
fn unreaped(held: BorrowedFd<'_>) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

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
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

/// `/proc/<pid>/stat`'s `utime` and `stime` (fields 14 and 15: the process's own, not the
/// `cutime`/`cstime` of children it waited for), `starttime` (field 22) and `rss` pages
/// (field 24), read at once, then its storage I/O ([`storage_io`]). An exited process keeps
/// both until it is reaped; `None` once it is. An exited one (state `Z` or `X`) holds no memory.
pub(super) fn own_counters(pid: libc::pid_t) -> io::Result<Option<OwnCounters>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            // An unmounted procfs proves nothing about `pid`.
            std::fs::metadata("/proc/self/stat")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let invalid = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid}'s stat {stat:?} {what}"),
        )
    };
    // The command name is parenthesized and may hold anything; field 3 follows the last ')'.
    // Walk only the required fields, once, without allocating a vector of every field.
    let mut fields = stat
        .rfind(')')
        .map(|closing| stat[closing + 1..].split_whitespace())
        .ok_or_else(|| invalid("has no command delimiter"))?;
    let field = |value: Option<&str>| -> io::Result<u64> {
        value
            .ok_or_else(|| invalid("is too short"))?
            .parse()
            .map_err(|_| invalid("holds a field that is no count"))
    };
    let state = fields.next().ok_or_else(|| invalid("has no state"))?;
    let user_ticks = field(fields.nth(10))?; // Field 14, after the consumed state (field 3).
    let sys_ticks = field(fields.next())?; // Field 15.
    let started = field(fields.nth(6))?; // Field 22, after field 15.
    let exited = matches!(state, "Z" | "X");
    let resident_pages = if exited {
        0
    } else {
        field(fields.nth(1))? // Field 24, after field 22.
    };
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let hertz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let hertz = u32::try_from(hertz)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| io::Error::other(format!("the clock tick rate {hertz} is no rate")))?;
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = u64::try_from(page)
        .map_err(|_| io::Error::other(format!("the page size {page} is no size")))?;
    let cpu = |ticks| CpuMicros::of_ticks(ticks, 1_000_000_000, hertz).map_err(unit);
    let resident = if exited {
        ResidentBytes::ZERO
    } else {
        let bytes = resident_pages
            .checked_mul(page)
            .ok_or_else(|| invalid("holds more resident bytes than a u64 counts"))?;
        ResidentBytes::new(bytes).map_err(unit)?
    };
    let Some(io) = storage_io(pid)? else {
        return Ok(None);
    };
    Ok(Some(OwnCounters {
        started: StartStamp(started),
        cpu_user: cpu(user_ticks)?,
        cpu_sys: cpu(sys_ticks)?,
        resident,
        io,
        exited,
    }))
}

/// `/proc/<pid>/io`'s `read_bytes` (reads the process caused to be fetched from storage; its
/// page-cache hits count none) and `write_bytes` (pages it dirtied for storage, counted when
/// dirtied, not at writeback). A refused read is [`ProcessIoUnavailable::NotPermitted`]; a
/// missing file, of a process whose stat was just read, is a kernel without per-task I/O
/// accounting -- unless the process was reaped meanwhile, which the caller's fence finds.
/// `None` when the kernel says the process is gone (`ESRCH`).
fn storage_io(pid: libc::pid_t) -> io::Result<Option<ProcessStorageIo>> {
    let text = match std::fs::read_to_string(format!("/proc/{pid}/io")) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            return Ok(Some(ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotPermitted,
            }));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Some(ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotAccounted,
            }));
        }
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        Err(error) => return Err(error),
    };
    let counter = |name: &str| -> io::Result<StorageIoBytes> {
        let value = text
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("process {pid}'s io {text:?} has no {name}"),
                )
            })?;
        let value = value.trim().parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid}'s io {name} {value:?} is no count"),
            )
        })?;
        StorageIoBytes::new(value).map_err(unit)
    };
    Ok(Some(ProcessStorageIo::Read {
        read_bytes: counter("read_bytes")?,
        write_bytes: counter("write_bytes")?,
    }))
}
