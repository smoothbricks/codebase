//! The pre-exec observation fence (86-hts-shell-subtasks.md B23b/B23d).
//! Only async-signal-safe calls run in the child. The supervisor attaches the selected native
//! source while the child is held, then releases it; a refused attachment fails the exec.

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// The supervisor waits for a pid here, then releases that same held child.
pub(crate) struct LeaderGate {
    ready: OwnedFd,
    go: OwnedFd,
}

/// Passed to the process that forks the workload. Both descriptors close at exec.
pub(crate) struct HeldEnds {
    pub(crate) ready: OwnedFd,
    pub(crate) go: OwnedFd,
}

pub(crate) fn leader_gate() -> io::Result<(LeaderGate, HeldEnds)> {
    let (ready_read, ready_write) = close_on_exec_pipe()?;
    let (go_read, go_write) = close_on_exec_pipe()?;
    Ok((
        LeaderGate {
            ready: ready_read,
            go: go_write,
        },
        HeldEnds {
            ready: ready_write,
            go: go_read,
        },
    ))
}

#[cfg(target_os = "linux")]
fn close_on_exec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: space for both descriptors; pipe2 creates them atomically close-on-exec.
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: these fresh descriptors have no other owners.
    Ok(unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) })
}

#[cfg(not(target_os = "linux"))]
fn close_on_exec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    crate::fork_lock::create(|| {
        let mut ends = [0; 2];
        // SAFETY: space for both descriptors, protected from a concurrent fork until CLOEXEC.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: these fresh descriptors have no other owners.
        let (read, write) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        for end in [&read, &write] {
            // SAFETY: a plain fcntl of this owned descriptor.
            if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok((read, write))
    })
}

/// Called between fork and exec: report this pid and wait for the observer's byte.
/// The implementation allocates nothing and calls only getpid/write/read (async-signal-safe).
pub fn hold_before_exec(ready: RawFd, go: RawFd) -> io::Result<()> {
    // SAFETY: getpid has no preconditions.
    let pid = unsafe { libc::getpid() }.to_ne_bytes();
    loop {
        // SAFETY: pid is readable for its full length, smaller than PIPE_BUF.
        let count = unsafe { libc::write(ready, pid.as_ptr().cast(), pid.len()) };
        if count == 4 {
            break;
        }
        if count >= 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    let mut byte = 0_u8;
    loop {
        // SAFETY: one writable byte, the gate descriptor inherited before the fork.
        match unsafe { libc::read(go, (&raw mut byte).cast(), 1) } {
            1 => return Ok(()),
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

impl LeaderGate {
    /// EOF before a report means the spawn failed before its hook. No process is invented.
    /// A partial report or a failed attachment closes the release pipe and fails the exec.
    pub(crate) fn observe(
        self,
        watch: impl FnOnce(u32) -> io::Result<()>,
    ) -> io::Result<Option<u32>> {
        let mut ready = std::fs::File::from(self.ready);
        let mut pid = [0_u8; 4];
        match ready.read(&mut pid[..1])? {
            0 => return Ok(None),
            1 => ready.read_exact(&mut pid[1..])?,
            _ => unreachable!("a one-byte read cannot exceed its buffer"),
        }
        let pid = u32::try_from(i32::from_ne_bytes(pid))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        watch(pid)?;
        std::fs::File::from(self.go).write_all(b"x")?;
        Ok(Some(pid))
    }
}
