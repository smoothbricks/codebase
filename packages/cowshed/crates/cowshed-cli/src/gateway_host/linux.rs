//! systemd signals a unit's main process only (`KillMode=process`), so the kernel is what ties
//! the daemon to its host: the daemon asks for SIGKILL at its parent's death.

use std::io;

/// Run in the child between fork and exec: have the kernel SIGKILL this process when the host,
/// `host`, dies, so a host the manager kills at the unit's stop timeout never leaves a daemon
/// holding the control socket. The setting survives the exec. A host that died before it took
/// effect is noticed by the parent this process has now, and the exec is refused with `ESRCH`.
///
/// Async-signal-safe: `prctl` and `getppid` only, and an error that allocates nothing.
pub(super) fn end_with_host(host: libc::pid_t) -> io::Result<()> {
    // SAFETY: PR_SET_PDEATHSIG takes a signal number and reads no memory.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: reads this process's parent pid.
    if unsafe { libc::getppid() } == host {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(libc::ESRCH))
    }
}
