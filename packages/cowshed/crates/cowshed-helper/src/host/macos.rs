//! macOS: the kernel answers a mount's type and source through `fstatfs`.

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

/// OpenZFS on macOS installs under `/usr/local/zfs`; the rest is the system's own.
pub const TRUSTED_PATH: &str = "/usr/local/zfs/bin:/usr/sbin:/sbin:/usr/bin:/bin";

/// `f_mntfromname` of `directory`'s filesystem when its type is `zfs`. With
/// `com.apple.devdisk=off` and `com.apple.mimic=off` (01b_zfs.md, "No disk per dataset") that is
/// the dataset name; a dataset mounted through a proxy disk reports `/dev/disk…` and so never
/// equals one.
pub fn zfs_mount_source(directory: &File) -> io::Result<Option<String>> {
    let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `directory` is an open descriptor and `filesystem` is a writable `statfs`.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fstatfs` returned 0, so it filled `filesystem`.
    let filesystem = unsafe { filesystem.assume_init() };
    // SAFETY: the kernel NUL-terminates both fixed-size name fields.
    let (fstype, source) = unsafe {
        (
            CStr::from_ptr(filesystem.f_fstypename.as_ptr()),
            CStr::from_ptr(filesystem.f_mntfromname.as_ptr()),
        )
    };
    Ok((fstype.to_bytes() == b"zfs").then(|| source.to_string_lossy().into_owned()))
}
