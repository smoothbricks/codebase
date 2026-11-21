//! What the wire protocol's descriptor passing takes from Linux.

use std::os::fd::OwnedFd;

/// `recvmsg` flags for a frame that carries descriptors: Linux installs every received
/// descriptor close-on-exec atomically, including the cgroup placement authority, so no command
/// can inherit it through a concurrent exec.
pub(super) const RECEIVE_FLAGS: libc::c_int = libc::MSG_CMSG_CLOEXEC;

/// Nothing to mark: [`RECEIVE_FLAGS`] already installed the descriptor close-on-exec.
pub(super) fn close_on_exec(_descriptor: &OwnedFd) {}

/// Clear this thread's `errno`. A plain store; async-signal-safe.
pub(crate) fn clear_errno() {
    // SAFETY: the location is this thread's own errno.
    unsafe {
        *libc::__errno_location() = 0;
    }
}
