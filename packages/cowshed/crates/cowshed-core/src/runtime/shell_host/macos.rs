//! What the wire protocol's descriptor passing and nice readback take from XNU.

use std::os::fd::{AsRawFd as _, OwnedFd};

/// `recvmsg` flags for a frame that carries descriptors: XNU has no atomic close-on-exec for
/// received descriptors, so [`close_on_exec`] marks each one after it arrives.
pub(super) const RECEIVE_FLAGS: libc::c_int = 0;

/// Mark a received descriptor close-on-exec.
pub(super) fn close_on_exec(descriptor: &OwnedFd) {
    // SAFETY: plain fcntl on an owned descriptor.
    unsafe {
        libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
    }
}

/// Clear this thread's `errno`. A plain store; async-signal-safe.
pub(crate) fn clear_errno() {
    // SAFETY: the location is this thread's own errno.
    unsafe {
        *libc::__error() = 0;
    }
}
