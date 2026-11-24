//! The lines of Linux's `/proc/locks` (fs/locks.c, `lock_get_status`): the kernel's record of
//! every file lock, each naming its file by device (`MAJOR:MINOR` in hex) and inode (decimal).
//! A pure reading of the text.

use std::io;

use super::{HeldLock, LockKind};

/// A file as /proc/locks names it: its device's major and minor numbers and its inode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileId {
    pub(crate) major: u32,
    pub(crate) minor: u32,
    pub(crate) inode: u64,
}

/// The locks `table` records on `file`, in the table's order. A line `->` marks is a process
/// waiting for a lock, not holding one. A pid of 0 (a holder outside the reader's pid namespace)
/// or -1 (an open-file-description lock) names no process. A line that is no lock record is an
/// error, never skipped: a format the kernel changed must not read as an unlocked file.
pub(crate) fn locks_of(table: &str, file: FileId) -> io::Result<Vec<HeldLock>> {
    let mut locks = Vec::new();
    for line in table.lines().filter(|line| !line.trim().is_empty()) {
        let (named, lock) = record(line).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("/proc/locks holds a line that is no lock record: {line:?}"),
            )
        })?;
        if named == file {
            locks.push(lock);
        }
    }
    Ok(locks)
}

/// One record: `<id>: [->] <kind> <mode> <access> <pid> <major>:<minor>:<inode> <start> <end>`.
fn record(line: &str) -> Option<(FileId, HeldLock)> {
    let mut fields = line.split_ascii_whitespace();
    fields.next()?.strip_suffix(':')?;
    let mut kind = fields.next()?;
    let waiting = kind == "->";
    if waiting {
        kind = fields.next()?;
    }
    // ADVISORY, MANDATORY, or a lease's ACTIVE, BREAKING, BREAKER or INACTIVE.
    fields.next()?;
    let access = fields.next()?;
    let pid: i64 = fields.next()?.parse().ok()?;
    let mut id = fields.next()?.split(':');
    let file = FileId {
        major: u32::from_str_radix(id.next()?, 16).ok()?,
        minor: u32::from_str_radix(id.next()?, 16).ok()?,
        inode: id.next()?.parse().ok()?,
    };
    if id.next().is_some() {
        return None;
    }
    let lock = HeldLock {
        holder: libc::pid_t::try_from(pid).ok().filter(|pid| *pid > 0),
        kind: Some(match kind {
            "POSIX" => LockKind::Posix,
            "FLOCK" => LockKind::Flock,
            "OFDLCK" => LockKind::OpenFileDescription,
            "LEASE" => LockKind::Lease,
            other => LockKind::Other(other.to_owned()),
        }),
        exclusive: access == "WRITE",
        waiting,
    };
    Some((file, lock))
}
