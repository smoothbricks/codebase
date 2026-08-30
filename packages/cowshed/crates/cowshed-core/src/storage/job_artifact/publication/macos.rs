use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd};

use super::{ArtifactError, Parent, PublicationStage, publication_error};

pub(super) fn try_fast_clone(
    parent: &mut Parent,
    source: &File,
) -> Result<Option<File>, ArtifactError> {
    let result = unsafe {
        libc::fclonefileat(
            source.as_raw_fd(),
            parent.directory.as_raw_fd(),
            parent.temporary_leaf.as_ptr(),
            0,
        )
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        // Match Darwin's raw errno values directly: std::io::ErrorKind does not expose these
        // filesystem conditions on all supported Rust versions.
        if error.raw_os_error().is_some_and(|code| {
            code == libc::EXDEV
                || code == libc::ENOTSUP
                || code == libc::EACCES
                || code == libc::EPERM
        }) {
            return Ok(None);
        }
        return Err(publication_error(
            &parent.temporary_path(),
            PublicationStage::Clone,
            error,
        ));
    }
    parent.temporary_exists = true;
    let fd = unsafe {
        libc::openat(
            parent.directory.as_raw_fd(),
            parent.temporary_leaf.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(publication_error(
            &parent.temporary_path(),
            PublicationStage::Clone,
            io::Error::last_os_error(),
        ));
    }
    if unsafe { libc::fchmod(fd, 0o600) } != 0 {
        let error = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(publication_error(
            &parent.temporary_path(),
            PublicationStage::Clone,
            error,
        ));
    }
    Ok(Some(unsafe { File::from_raw_fd(fd) }))
}
