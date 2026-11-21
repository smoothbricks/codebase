//! The create-new rename as Linux offers it: `renameat2` with `RENAME_NOREPLACE`.

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;

/// Rename `from_name` beneath `from` to `to_name` beneath `to`, refusing an existing destination
/// atomically and following neither leaf.
pub(super) fn rename_noreplace_between(
    from: &File,
    from_name: &CStr,
    to: &File,
    to_name: &CStr,
) -> io::Result<()> {
    // SAFETY: both directory fds are held and both leaves are NUL-terminated for the call.
    // RENAME_NOREPLACE makes the destination check part of the rename. The raw syscall avoids
    // depending on glibc's renameat2 wrapper.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            from.as_raw_fd(),
            from_name.as_ptr(),
            to.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
