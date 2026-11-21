//! Darwin's part of the filesystem primitives: `renameatx_np` for a rename that never replaces
//! and for an exchange, `F_GETPATH` for a held directory's current name, and `__error` for this
//! thread's `errno`.

use std::ffi::{CStr, OsString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::ffi::OsStringExt as _;
use std::path::PathBuf;

/// Zero this thread's `errno`, which alone tells the end of a `readdir` stream from its failure.
pub(super) fn clear_errno() {
    // SAFETY: errno is thread-local; zero distinguishes readdir EOF from failure.
    unsafe { *libc::__error() = 0 };
}

/// The kernel's current name for the held `directory` (`F_GETPATH`).
pub(super) fn current_path(directory: &File) -> io::Result<PathBuf> {
    let mut path = vec![0_u8; usize::try_from(libc::PATH_MAX).expect("PATH_MAX fits usize")];
    // SAFETY: F_GETPATH fills at most PATH_MAX bytes of the live directory's path.
    if unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let length = CStr::from_bytes_until_nul(&path)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .to_bytes()
        .len();
    path.truncate(length);
    Ok(PathBuf::from(OsString::from_vec(path)))
}

/// Atomically rename one directory-relative entry without replacing an existing destination.
pub fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination_directory: std::os::fd::RawFd,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the call; `directory` is an open
    // directory descriptor owned by the caller. RENAME_EXCL makes the destination check atomic.
    let result = unsafe {
        libc::renameatx_np(
            directory,
            source.as_ptr(),
            destination_directory,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Atomically exchange two directory entries, without following either entry's symlink
/// (`RENAME_SWAP`). A failure leaves both entries unchanged.
pub fn rename_exchange(directory: RawFd, source: &CStr, destination: &CStr) -> io::Result<()> {
    // SAFETY: the NUL-terminated names remain live for the call. The caller owns the directory
    // descriptor, or passes AT_FDCWD. RENAME_SWAP exchanges entries in one kernel operation.
    let result = unsafe {
        libc::renameatx_np(
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
