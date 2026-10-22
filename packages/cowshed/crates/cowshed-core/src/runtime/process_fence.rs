//! The macOS pre-exec observation fence (07_api.md, "Process-tree observations").
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

impl HeldEnds {
    /// The hold a child forked in this same process runs. That child inherits every descriptor
    /// open at its fork, the gate's own ends included, and while it keeps the gate's release end
    /// a gate dropped without releasing never reads as the end of that pipe: the child would wait
    /// for a release nobody can send, and the spawn for its exec. So it closes the gate's ends
    /// first. A child forked by another process (a warm host) never had them.
    pub(crate) fn in_process(&self, gate: &LeaderGate) -> InProcessHold {
        InProcessHold {
            ready: self.ready.as_raw_fd(),
            go: self.go.as_raw_fd(),
            gate: [gate.ready.as_raw_fd(), gate.go.as_raw_fd()],
        }
    }
}

/// The descriptor numbers a held child of this process uses between fork and exec: its report
/// and release ends, and the gate's ends it inherited and closes before it waits.
#[derive(Clone, Copy)]
pub(crate) struct InProcessHold {
    ready: RawFd,
    go: RawFd,
    gate: [RawFd; 2],
}

impl InProcessHold {
    /// Called between fork and exec. Allocates nothing and calls only close/getpid/write/read
    /// (async-signal-safe).
    pub(crate) fn hold(self) -> io::Result<()> {
        for end in self.gate {
            // SAFETY: this child's own copy of a gate end; no other process's descriptor changes.
            // Darwin and Linux release the descriptor even when close reports EINTR.
            if unsafe { libc::close(end) } == -1 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
        hold_before_exec(self.ready, self.go)
    }
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

fn close_on_exec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    cowshed_os::fork_lock::create(|| {
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

/// Called between fork and exec: report this pid and wait for the observer's byte. The end of the
/// release pipe before that byte means the observer withdrew the release: ECANCELED, the errno
/// the parent's spawn then fails with. The implementation allocates nothing and calls only
/// getpid/write/read (async-signal-safe).
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
            0 => return Err(io::Error::from_raw_os_error(libc::ECANCELED)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    use std::os::unix::process::CommandExt as _;

    /// A refused attachment withdraws the release: the held child reads the end of the release
    /// pipe, its exec fails, and the spawn returns instead of waiting for an exec that never
    /// comes (07_api.md, "Process-tree observations").
    #[test]
    fn a_refused_attachment_fails_the_held_exec() {
        let (gate, held) = leader_gate().expect("the fence's pipes");
        let hold = held.in_process(&gate);
        let mut command = std::process::Command::new("/usr/bin/true");
        // SAFETY: the hold calls only close/getpid/write/read, all async-signal-safe.
        unsafe {
            command.pre_exec(move || hold.hold());
        }
        std::thread::scope(|scope| {
            let spawning = scope.spawn(move || {
                let child = command.spawn_locked();
                drop(held);
                child
            });
            let observed = gate.observe(|_| Err(io::Error::other("the attachment is refused")));
            let spawned = spawning.join().expect("the spawning thread");
            assert_eq!(
                observed
                    .expect_err("the refusal is the observation's answer")
                    .to_string(),
                "the attachment is refused"
            );
            assert_eq!(
                spawned
                    .expect_err("a withdrawn release fails the exec")
                    .raw_os_error(),
                Some(libc::ECANCELED)
            );
        });
    }
}
