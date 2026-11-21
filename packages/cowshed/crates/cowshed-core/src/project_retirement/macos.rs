//! The create-new rename as XNU offers it: `renameatx_np` with `RENAME_EXCL`.

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
    // RENAME_EXCL makes the destination check part of the rename.
    let result = unsafe {
        libc::renameatx_np(
            from.as_raw_fd(),
            from_name.as_ptr(),
            to.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
