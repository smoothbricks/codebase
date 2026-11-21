//! How the manager waits for a supervisor that is not its child on Linux: a pidfd, readable once
//! its process has exited.

use std::io;

/// Block until `pid`, which is not this process's child, exits. A process already gone has
/// exited.
pub(super) fn wait_for_exit(pid: u32) -> io::Result<()> {
    let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: pidfd_open returns a new descriptor this function owns and closes.
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if descriptor < 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(error)
        };
    }
    let descriptor = i32::try_from(descriptor).map_err(io::Error::other)?;
    let mut poll = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `poll` describes one live descriptor this function owns.
    let waited = unsafe { libc::poll(&mut poll, 1, -1) };
    // SAFETY: closing the descriptor opened above.
    unsafe { libc::close(descriptor) };
    if waited < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
