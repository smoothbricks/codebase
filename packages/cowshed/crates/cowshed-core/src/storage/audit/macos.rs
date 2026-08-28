use std::ffi::CStr;
use std::io;
use std::os::fd::RawFd;

pub(super) fn rename_noreplace(
    directory: RawFd,
    temporary: &CStr,
    sealed: &CStr,
) -> io::Result<()> {
    let result = unsafe {
        libc::renameatx_np(
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
