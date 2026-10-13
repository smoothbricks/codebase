//! The kernel's mount table: every mounted filesystem's mountpoint, source device and flags, as
//! `getfsstat` reports it now.

use std::io;
use std::path::PathBuf;

use crate::MountAccess;

const MNT_DONTBROWSE: u64 = 0x0010_0000;
const MNT_IGNORE_OWNERS: u64 = 0x0020_0000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelMountSnapshot {
    pub mount_id: u64,
    pub mount_point: PathBuf,
    pub source_device: String,
    flags: u64,
}

impl KernelMountSnapshot {
    pub fn new(
        mount_id: u64,
        mount_point: impl Into<PathBuf>,
        source_device: impl Into<String>,
        nobrowse: bool,
        owners: bool,
    ) -> Self {
        let mut flags = 0;
        if nobrowse {
            flags |= MNT_DONTBROWSE;
        }
        if !owners {
            flags |= MNT_IGNORE_OWNERS;
        }
        Self {
            mount_id,
            mount_point: mount_point.into(),
            source_device: source_device.into(),
            flags,
        }
    }

    /// The access this mount was made with: what mounting the volume again must ask for.
    pub fn access(&self) -> MountAccess {
        MountAccess::from_kernel_flags(self.flags)
    }

    /// Whether this mount shows in the Finder: what mounting the volume again must ask for.
    pub fn browse(&self) -> bool {
        self.flags & MNT_DONTBROWSE == 0
    }

    /// Whether the mount honours file owners, as every cowshed volume mount must.
    pub fn owners_honoured(&self) -> bool {
        self.flags & MNT_IGNORE_OWNERS == 0
    }
}

/// The mount count Darwin `getfsstat` answered, which is `-1` on error with `errno` set.
/// Treating a negative count as a slice length would be memory-unsafe; `0` is
/// not a legitimate Darwin mount table either (at least `/` is mounted).
///
/// Only [`kernel_mounts`]' Darwin arm calls this, so the non-macOS lib must not compile it;
/// `test` keeps the predicate's own unit test on every platform.
#[cfg(any(target_os = "macos", test))]
fn mount_count(count: libc::c_int) -> Option<usize> {
    usize::try_from(count).ok().filter(|&count| count > 0)
}

/// Every mount the kernel holds now. Empty off macOS, where no APFS image is ever mounted.
pub fn kernel_mounts() -> io::Result<Vec<KernelMountSnapshot>> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::{CStr, OsStr};
        use std::hash::{Hash, Hasher};
        use std::os::unix::ffi::OsStrExt;

        let failed =
            || io::Error::other(format!("getfsstat failed: {}", io::Error::last_os_error()));
        let overflows =
            |room: usize| io::Error::other(format!("room for {room} mounts overflows getfsstat"));
        // Into a buffer this call owns. `getmntinfo` answers a process-static array that the
        // next `getmntinfo` on any thread overwrites, and mount reads run on several threads at
        // once: a `new` mounts its checkout and its build volume together, and a scratch root
        // releases its images together.
        // SAFETY: a null buffer asks `getfsstat` only for the number of mounts.
        let listed = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        // Room for mounts that appear between the two calls.
        let mut room = mount_count(listed).ok_or_else(failed)? + 8;
        let mut entries: Vec<libc::statfs> = Vec::new();
        loop {
            let bytes = room
                .checked_mul(std::mem::size_of::<libc::statfs>())
                .and_then(|bytes| libc::c_int::try_from(bytes).ok())
                .ok_or_else(|| overflows(room))?;
            // `entries` is empty, so this leaves room for `room` records.
            entries.reserve_exact(room);
            // SAFETY: `entries` has room for `room` records, `bytes` long, of which `getfsstat`
            // writes at most `bytes`, and it answers how many records it wrote, or `-1`.
            let count = unsafe { libc::getfsstat(entries.as_mut_ptr(), bytes, libc::MNT_NOWAIT) };
            let count = mount_count(count).ok_or_else(failed)?;
            if count < room {
                // SAFETY: `getfsstat` initialized the first `count` records.
                unsafe { entries.set_len(count) };
                break;
            }
            // A full buffer may have cut mounts off: read the table again into twice the room.
            room = room.checked_mul(2).ok_or_else(|| overflows(room))?;
        }
        Ok(entries
            .iter()
            .map(|entry| {
                // SAFETY: Darwin NUL-terminates `f_mntonname`/`f_mntfromname`
                // (`char[MAXPATHLEN]`), in a record `entries` owns.
                let bytes = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) }.to_bytes();
                // SAFETY: same NUL-termination and ownership as `f_mntonname`.
                let source_device = unsafe { CStr::from_ptr(entry.f_mntfromname.as_ptr()) }
                    .to_string_lossy()
                    .into_owned();
                let mount_point = PathBuf::from(OsStr::from_bytes(bytes));
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                mount_point.hash(&mut hasher);
                KernelMountSnapshot {
                    mount_id: hasher.finish(),
                    mount_point,
                    source_device,
                    flags: u64::from(entry.f_flags),
                }
            })
            .collect())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_mount_counts_refuse_errors_and_empty_tables() {
        for (count, mounts) in [(-1, None), (0, None), (1, Some(1))] {
            assert_eq!(mount_count(count), mounts, "getfsstat count={count}");
        }
    }
}
