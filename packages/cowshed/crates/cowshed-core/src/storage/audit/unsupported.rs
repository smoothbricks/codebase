use std::ffi::CStr;
use std::io;
use std::os::fd::RawFd;

pub(super) fn rename_noreplace(
    _directory: RawFd,
    _temporary: &CStr,
    _sealed: &CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic create-new rename is unsupported",
    ))
}
