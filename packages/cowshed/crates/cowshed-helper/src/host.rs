//! What the helper asks of the host it runs on. Each platform answers in its own file; nothing
//! outside this module names a platform.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// The `PATH` the helper gives `zfs`: system directories only, never the caller's.
pub use platform::TRUSTED_PATH;

/// The root-owned directory `cowshed setup` installs the helper in, read-only and root's: no
/// user can replace what `sudo` admits there (09_substrates.md, "Privileged helper").
pub use platform::SYSTEM_DIRECTORY;

/// The ZFS dataset `directory` is the mounted root of, when it is one.
pub fn mounted_dataset(directory: &File) -> io::Result<Option<String>> {
    let here = directory.metadata()?;
    // A mounted root's parent lies on another filesystem.
    let mut parent = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `directory` is an open descriptor, the name is a NUL-terminated literal and
    // `parent` is a writable `stat`.
    let status = unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            c"..".as_ptr(),
            parent.as_mut_ptr(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fstatat` returned 0, so it filled `parent`.
    let parent = unsafe { parent.assume_init() };
    #[allow(clippy::unnecessary_cast)] // `dev_t` is `i32` on macOS and `u64` on Linux.
    if parent.st_dev as u64 == here.dev() {
        return Ok(None);
    }
    platform::zfs_mount_source(directory)
}

/// Give `directory` to `uid:gid`.
pub fn give(directory: &File, uid: u32, gid: u32) -> io::Result<()> {
    // SAFETY: `directory` is an open descriptor.
    if unsafe { libc::fchown(directory.as_raw_fd(), uid, gid) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// The user the helper acts for: the real user of a setuid run, the invoking user of a `sudo`
/// run, and otherwise root itself. `SUDO_UID` is read only when the real user is already root,
/// who could claim any uid anyway.
pub fn caller() -> (u32, u32) {
    // SAFETY: both calls only read this process's credentials.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    if uid != 0 {
        return (uid, gid);
    }
    let sudo = |name| std::env::var(name).ok()?.parse::<u32>().ok();
    match (sudo("SUDO_UID"), sudo("SUDO_GID")) {
        (Some(uid), Some(gid)) => (uid, gid),
        _ => (0, 0),
    }
}

/// Whether this process may mutate datasets and mount: the helper refuses to start otherwise,
/// since a `zfs` failure further down would name a permission, not the missing install.
pub fn is_root() -> bool {
    // SAFETY: reads this process's effective uid.
    unsafe { libc::geteuid() == 0 }
}
