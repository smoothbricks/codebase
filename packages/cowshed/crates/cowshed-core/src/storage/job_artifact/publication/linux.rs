use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd};

use super::{ArtifactError, Parent, PublicationStage, publication_error};

pub(super) fn try_fast_clone(
    parent: &mut Parent,
    source: &File,
) -> Result<Option<File>, ArtifactError> {
    const FICLONE: libc::c_ulong = 0x4004_9409;
    let file = parent.create_temporary()?;
    if unsafe { libc::ioctl(file.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
        return Ok(Some(file));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error().is_some_and(|code| {
        code == libc::EXDEV
            || code == libc::EOPNOTSUPP
            || code == libc::ENOTTY
            || code == libc::EINVAL
    }) {
        drop(file);
        parent.cleanup_temporary().map_err(|cleanup| {
            publication_error(&parent.temporary_path(), PublicationStage::Cleanup, cleanup)
        })?;
        return Ok(None);
    }
    Err(publication_error(
        &parent.temporary_path(),
        PublicationStage::Clone,
        error,
    ))
}

pub(super) fn rename_noreplace(
    directory_fd: libc::c_int,
    temporary: &CStr,
    destination: &CStr,
) -> Result<libc::c_int, ArtifactError> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    Ok(unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory_fd,
            temporary.as_ptr(),
            directory_fd,
            destination.as_ptr(),
            RENAME_NOREPLACE,
        ) as libc::c_int
    })
}
