//! The children procinfo's tests spawn. Each is killed and reaped when its test ends, however it
//! ends, so a failed assertion leaves no process behind.

use std::io;
use std::ops::{Deref, DerefMut};
use std::process::{Child, Command};

/// A spawned child, killed and reaped when dropped unless the test reaped it first.
pub(crate) struct Reaped(Child);

pub(crate) trait SpawnReaped {
    fn spawn_reaped(&mut self) -> io::Result<Reaped>;
}

impl SpawnReaped for Command {
    fn spawn_reaped(&mut self) -> io::Result<Reaped> {
        self.spawn().map(Reaped)
    }
}

impl Deref for Reaped {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for Reaped {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Reaped {
    fn drop(&mut self) {
        // A child the test reaped has a status already, and is neither killed nor waited again.
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        if let Err(error) = self.0.kill() {
            eprintln!("procinfo test: cannot kill child {}: {error}", self.0.id());
        }
        if let Err(error) = self.0.wait() {
            eprintln!("procinfo test: cannot reap child {}: {error}", self.0.id());
        }
    }
}

/// Block until the child `pid` has exited, leaving it unreaped: a zombie its parent still holds.
pub(crate) fn exited_unreaped(pid: u32) -> io::Result<()> {
    // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is live storage for the call to fill.
    let answered = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            &raw mut info,
            libc::WEXITED | libc::WNOWAIT,
        )
    };
    if answered == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
