//! Linux's part of the filesystem primitives: `renameat2` for a rename that never replaces and
//! for an exchange, procfs for a held directory's current name, `statx` for the mount an inode
//! is on, `O_PATH` for a name held without opening what it names, and `__errno_location` for
//! this thread's `errno`.

use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

/// Zero this thread's `errno`, which alone tells the end of a `readdir` stream from its failure.
pub(super) fn clear_errno() {
    // SAFETY: errno is thread-local; zero distinguishes readdir EOF from failure.
    unsafe { *libc::__errno_location() = 0 };
}

/// The kernel's current name for the held `directory`: the link procfs keeps for its descriptor.
pub(super) fn current_path(directory: &File) -> io::Result<PathBuf> {
    fs::read_link(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}

/// The Linux mount identity of an already-held inode, not its backing device number.
/// A bind mount has a distinct identity even when both paths share the same filesystem.
pub fn mount_id(held: &File) -> io::Result<u64> {
    use rustix::fs::{AtFlags, StatxFlags, statx};
    let stat = statx(held, c"", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID)?;
    if !StatxFlags::from_bits_retain(stat.stx_mask).contains(StatxFlags::MNT_ID) {
        return Err(io::Error::other(
            "cannot prove mount boundary: STATX_MNT_ID unavailable",
        ));
    }
    Ok(stat.stx_mnt_id)
}

/// Atomically rename one directory-relative entry without replacing an existing destination.
pub fn rename_noreplace(
    directory: std::os::fd::RawFd,
    source: &std::ffi::CStr,
    destination_directory: std::os::fd::RawFd,
    destination: &std::ffi::CStr,
) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated and remain live for the syscall; `directory` is an
    // open directory descriptor owned by the caller. libc supplies the kernel ABI flag value.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            source.as_ptr(),
            destination_directory,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Atomically exchange two directory entries, without following either entry's symlink
/// (`RENAME_EXCHANGE`). A failure leaves both entries unchanged.
pub fn rename_exchange(directory: RawFd, source: &CStr, destination: &CStr) -> io::Result<()> {
    // SAFETY: the NUL-terminated names remain live for the syscall. The caller owns the directory
    // descriptor, or passes AT_FDCWD. The syscall uses the kernel ABI flags, avoiding a
    // dependency on glibc's newer renameat2 wrapper.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            source.as_ptr(),
            directory,
            destination.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Hold `path` as a name only (`O_PATH`), following a final symlink: enough to stat it, read
/// its mount identity, bind it and tie a Landlock rule to it, with no permission on the file
/// itself and without opening what it names. A terminal with no session, a FIFO with no writer
/// and a file this user cannot read are all held.
///
/// This is the one place the flag is passed, and it goes to `open(2)` directly.
/// `std::fs::OpenOptions::custom_flags` clears the access-mode bits (`O_ACCMODE`) from what it
/// is given, and musl defines `O_ACCMODE` as `03 | O_SEARCH` with `O_SEARCH` equal to `O_PATH`:
/// built against musl, an `OpenOptions` asked for `O_PATH` makes a plain `O_RDONLY` open, which
/// reads, blocks and fails as one (`ENXIO` for `/dev/tty` in a process with no terminal).
pub fn open_path(path: &Path) -> io::Result<OwnedFd> {
    open_name(path, 0)
}

/// [`open_path`] that holds a final symlink itself instead of what it points at.
pub fn open_path_nofollow(path: &Path) -> io::Result<OwnedFd> {
    open_name(path, libc::O_NOFOLLOW)
}

fn open_name(path: &Path, link: libc::c_int) -> io::Result<OwnedFd> {
    let name = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
    loop {
        // SAFETY: `name` is NUL-terminated and outlives the call; no mode is read without
        // O_CREAT.
        let descriptor =
            unsafe { libc::open(name.as_ptr(), libc::O_PATH | libc::O_CLOEXEC | link) };
        if descriptor >= 0 {
            // SAFETY: the kernel just returned this descriptor to this call alone.
            return Ok(unsafe { OwnedFd::from_raw_fd(descriptor) });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file status flags of `held`, as the kernel keeps them.
    fn status_flags(held: &OwnedFd) -> libc::c_int {
        // SAFETY: F_GETFL reads the flags of a descriptor this test owns.
        let flags = unsafe { libc::fcntl(held.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0, "{}", io::Error::last_os_error());
        flags
    }

    /// The descriptor is a name, on glibc and on musl alike: the kernel reports `O_PATH` on it.
    /// Opened through `OpenOptions::custom_flags` it is not one under musl.
    #[test]
    fn a_held_name_is_an_o_path_descriptor() {
        for path in ["/", "/proc/self/exe", "/dev/null"] {
            for held in [
                open_path(Path::new(path)).expect(path),
                open_path_nofollow(Path::new(path)).expect(path),
            ] {
                assert_ne!(status_flags(&held) & libc::O_PATH, 0, "{path}");
            }
        }
    }

    /// `/proc/self/exe` is a link to this test's image: followed it is a regular file, held
    /// without following it is the link.
    #[test]
    fn a_final_symlink_is_followed_unless_refused() {
        let link = Path::new("/proc/self/exe");
        let followed = File::from(open_path(link).expect("follow the link"));
        assert!(followed.metadata().expect("stat").file_type().is_file());
        let held = File::from(open_path_nofollow(link).expect("hold the link"));
        assert!(held.metadata().expect("stat").file_type().is_symlink());
    }

    #[test]
    fn a_name_that_is_absent_or_holds_a_nul_is_refused() {
        let absent = open_path(Path::new("/nonexistent-cowshed-path")).expect_err("absent");
        assert_eq!(absent.kind(), io::ErrorKind::NotFound);
        let nul = open_path(Path::new("/tmp/a\0b")).expect_err("a NUL in the name");
        assert_eq!(nul.kind(), io::ErrorKind::InvalidInput);
    }
}
