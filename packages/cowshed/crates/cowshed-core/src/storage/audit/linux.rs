use std::ffi::CStr;
use std::io;
use std::os::fd::RawFd;

pub(super) fn rename_noreplace(
    directory: RawFd,
    temporary: &CStr,
    sealed: &CStr,
) -> io::Result<()> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
