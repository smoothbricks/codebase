#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafMaterialization {
    Cloned,
    Copied,
}

#[cfg(any(not(target_os = "macos"), test))]
pub(super) fn clone_fallback_error(error: &std::io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        code == libc::EXDEV
            || code == libc::EOPNOTSUPP
            || code == libc::ENOTTY
            || code == libc::EINVAL
    })
}

#[cfg(not(target_os = "macos"))]
pub use linux::{copy_leaf, directory_metadata};
#[cfg(target_os = "macos")]
pub use macos::{copy_leaf, directory_metadata};

/// Observe a logical hole in a freshly opened regular file. Physical compression alone is not
/// a hole: SEEK_HOLE answers in the logical data stream. Restore offset zero before the caller
/// verifies bytes; an unsupported observation is an error, never guessed from st_blocks.
pub(super) fn has_holes(file: &std::fs::File, length: u64) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    if length == 0 {
        return Ok(false);
    }
    // SAFETY: the file owns its descriptor through both seeks; neither consumes it.
    let hole = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
    if hole < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the same live descriptor is restored to the start before byte reads.
    if unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_SET) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let hole = u64::try_from(hole).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "negative logical hole offset",
        )
    })?;
    if hole > length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "logical hole offset exceeds the recorded file length",
        ));
    }
    Ok(hole < length)
}

#[cfg(test)]
mod tests {
    use super::clone_fallback_error;

    #[test]
    fn only_filesystem_clone_capability_errors_fall_back() {
        for code in [libc::EXDEV, libc::EOPNOTSUPP, libc::ENOTTY, libc::EINVAL] {
            assert!(clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
        for code in [libc::EIO, libc::ENOSPC, libc::EACCES] {
            assert!(!clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
    }
}
