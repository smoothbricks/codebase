//! macOS's answer to where a path's filesystem is mounted: `statfs`'s `f_mntonname`.

use std::path::{Path, PathBuf};

/// Where the filesystem holding `path` is mounted.
pub(super) fn mount_point_of(path: &Path) -> Option<PathBuf> {
    use std::ffi::{CStr, CString, OsStr};
    use std::os::unix::ffi::OsStrExt as _;

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statfs` is plain old data; all-zero is a valid value for the kernel to overwrite.
    let mut stats: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is NUL-terminated and `stats` is a valid, writable `statfs`.
    if unsafe { libc::statfs(path.as_ptr(), &mut stats) } != 0 {
        return None;
    }
    // SAFETY: Darwin NUL-terminates `f_mntonname` within its fixed-size array.
    let mounted = unsafe { CStr::from_ptr(stats.f_mntonname.as_ptr()) };
    Some(PathBuf::from(OsStr::from_bytes(mounted.to_bytes())))
}
