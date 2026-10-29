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

    /// The kernel's raw `statfs` flags, so an error about this mount can show what it observed.
    pub fn flags(&self) -> u64 {
        self.flags
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

/// Read one owned kernel snapshot, shared by namespace projection and exact FSID custody.
#[cfg(target_os = "macos")]
fn kernel_filesystems() -> io::Result<Vec<libc::statfs>> {
    let failed = |count| {
        if count < 0 {
            io::Error::last_os_error()
        } else {
            io::Error::other("getfsstat returned an empty kernel mount table")
        }
    };
    let overflows =
        |room: usize| io::Error::other(format!("room for {room} mounts overflows getfsstat"));
    // This buffer belongs to the caller; getmntinfo's process-static buffer races other readers.
    // SAFETY: a null buffer asks getfsstat only for the number of mounts.
    let listed = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    let mut room = mount_count(listed).ok_or_else(|| failed(listed))? + 8;
    let mut entries: Vec<libc::statfs> = Vec::new();
    loop {
        let bytes = room
            .checked_mul(std::mem::size_of::<libc::statfs>())
            .and_then(|bytes| libc::c_int::try_from(bytes).ok())
            .ok_or_else(|| overflows(room))?;
        entries.reserve_exact(room);
        // SAFETY: entries has room for room records, bytes long. getfsstat initializes at most
        // that many and returns the number written, or -1 with errno set.
        let count = unsafe { libc::getfsstat(entries.as_mut_ptr(), bytes, libc::MNT_NOWAIT) };
        let count = mount_count(count).ok_or_else(|| failed(count))?;
        if count < room {
            // SAFETY: getfsstat initialized the first count records.
            unsafe { entries.set_len(count) };
            return Ok(entries);
        }
        // A full buffer may have omitted mounts. Grow the same snapshot buffer, not an effect.
        room = room.checked_mul(2).ok_or_else(|| overflows(room))?;
    }
}

/// Every mount the kernel holds now. Empty off macOS, where no APFS image is ever mounted.
pub fn kernel_mounts() -> io::Result<Vec<KernelMountSnapshot>> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::{CStr, OsStr};
        use std::hash::{Hash, Hasher};
        use std::os::unix::ffi::OsStrExt;

        Ok(kernel_filesystems()?
            .iter()
            .map(|entry| {
                // SAFETY: Darwin NUL-terminates both names in records this snapshot owns.
                let bytes = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) }.to_bytes();
                // SAFETY: same NUL-termination and ownership as f_mntonname.
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

/// The kernel FSID returned by fstatfs/getfsstat, never a mountpoint hash or BSD spelling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemId([i32; 2]);

impl FilesystemId {
    /// A recording runner's precise filesystem identity.
    pub const fn new(value: [i32; 2]) -> Self {
        Self(value)
    }

    #[cfg(target_os = "macos")]
    fn from_native(value: libc::fsid_t) -> Self {
        // SAFETY: Darwin sys/_types/_fsid_t.h defines fsid_t as two int32_t values, and libc's
        // repr(C) fsid_t holds that same private [i32; 2]. This owned-value bitcopy preserves
        // every native-endian identity bit; all i32 pairs are valid and transmute checks size.
        Self(unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(value) })
    }
}

/// Force is an explicit existing release-policy edge after recognized busy and its grace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnmountMode {
    Normal,
    ForceAfterGrace,
}

/// The actual retired-descriptor observation, retained separately from kernel absence.
#[derive(Debug)]
pub enum PostUnmountFd {
    Dead(io::Error),
}

#[derive(Debug)]
pub enum UnmountEffect {
    NoEffect,
    Unmounted { post_fd: PostUnmountFd },
}

#[derive(Debug)]
pub enum NativeUnmountError {
    Busy(io::Error),
    Authority {
        expected: FilesystemId,
        found: FilesystemId,
    },
    KernelRead {
        stage: &'static str,
        source: io::Error,
    },
    Failed(io::Error),
    Postcondition {
        filesystem: FilesystemId,
    },
}

impl std::fmt::Display for NativeUnmountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(source) => write!(formatter, "kernel refused unmount as busy: {source}"),
            Self::Authority { expected, found } => write!(
                formatter,
                "held unmount root changed filesystem: expected {expected:?}, found {found:?}"
            ),
            Self::KernelRead { stage, source } => write!(formatter, "{stage}: {source}"),
            Self::Failed(source) => write!(formatter, "kernel unmount failed: {source}"),
            Self::Postcondition { filesystem } => write!(
                formatter,
                "kernel unmount did not retire filesystem {filesystem:?} and remove its mount"
            ),
        }
    }
}

impl std::error::Error for NativeUnmountError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Busy(source) | Self::Failed(source) | Self::KernelRead { source, .. } => {
                Some(source)
            }
            Self::Authority { .. } | Self::Postcondition { .. } => None,
        }
    }
}

/// A mount-root vnode held for object identity, with its original source and kernel FSID.
/// O_EVTONLY permits ordinary unmount without redirecting this descriptor to the directory
/// beneath it. A retired root remains dead; naming and unmount never reopen its pathname.
#[derive(Debug)]
pub struct HeldMount {
    source_device: String,
    is_root: bool,
    filesystem_id: FilesystemId,
    held: Option<cowshed_os::fork_lock::Fenced<std::fs::File>>,
}

impl HeldMount {
    /// What a recording runner states the kernel answered, holding nothing.
    pub fn stated(
        source_device: impl Into<String>,
        is_root: bool,
        filesystem_id: FilesystemId,
    ) -> Self {
        Self {
            source_device: source_device.into(),
            is_root,
            filesystem_id,
            held: None,
        }
    }

    /// The device of the filesystem the held path lies on.
    pub fn source_device(&self) -> &str {
        &self.source_device
    }

    /// Whether the held path is that filesystem's mount root, not a directory inside it.
    pub fn is_root(&self) -> bool {
        self.is_root
    }

    pub fn filesystem_id(&self) -> FilesystemId {
        self.filesystem_id
    }

    /// Called only through an OwnedVolumeRoot under its image lease, raw pin and namespace
    /// admission. The same descriptor binds the effect; neither BSD names nor paths select it.
    pub(crate) fn unmount(&self, mode: UnmountMode) -> Result<UnmountEffect, NativeUnmountError> {
        let Some(held) = self.held.as_ref() else {
            return Err(NativeUnmountError::Failed(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "a stated mount of {} holds no unmount descriptor",
                    self.source_device
                ),
            )));
        };
        if !self.is_root {
            return Err(NativeUnmountError::Failed(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a held filesystem root", self.source_device),
            )));
        }
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;

            let present = |stage| {
                kernel_filesystems()
                    .map(|entries| {
                        entries.iter().any(|entry| {
                            FilesystemId::from_native(entry.f_fsid) == self.filesystem_id
                        })
                    })
                    .map_err(|source| NativeUnmountError::KernelRead { stage, source })
            };
            if !present("read kernel mount table before unmount")? {
                return Ok(UnmountEffect::NoEffect);
            }
            let current =
                descriptor_filesystem(held).map_err(|source| NativeUnmountError::KernelRead {
                    stage: "read held filesystem before unmount",
                    source,
                })?;
            let found = FilesystemId::from_native(current.f_fsid);
            if found != self.filesystem_id {
                return Err(NativeUnmountError::Authority {
                    expected: self.filesystem_id,
                    found,
                });
            }
            let flags = match mode {
                UnmountMode::Normal => 0,
                UnmountMode::ForceAfterGrace => libc::MNT_FORCE,
            };
            // Public Darwin funmount is supplied by the licensed macOS SDK/libSystem. There
            // is no private syscall number, weak-link shim or pathname fallback.
            unsafe extern "C" {
                fn funmount(fd: libc::c_int, flags: libc::c_int) -> libc::c_int;
            }
            // SAFETY: held owns the proven root descriptor through this call and both
            // postcondition reads. flags is ordinary unmount or the explicit release edge.
            if unsafe { funmount(held.as_raw_fd(), flags) } != 0 {
                let source = io::Error::last_os_error();
                return Err(if source.raw_os_error() == Some(libc::EBUSY) {
                    NativeUnmountError::Busy(source)
                } else {
                    NativeUnmountError::Failed(source)
                });
            }
            let post_fd = match descriptor_filesystem(held) {
                Err(source) if source.raw_os_error() == Some(libc::ENOTSUP) => {
                    PostUnmountFd::Dead(source)
                }
                Err(source) => {
                    return Err(NativeUnmountError::KernelRead {
                        stage: "read held filesystem after unmount",
                        source,
                    });
                }
                Ok(_) => {
                    return Err(NativeUnmountError::Postcondition {
                        filesystem: self.filesystem_id,
                    });
                }
            };
            // A retired FD alone proves no absence. Only a successful independent kernel
            // table read can establish that the exact original filesystem departed.
            if present("read kernel mount table after unmount")? {
                return Err(NativeUnmountError::Postcondition {
                    filesystem: self.filesystem_id,
                });
            }
            Ok(UnmountEffect::Unmounted { post_fd })
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (held, mode);
            Err(NativeUnmountError::Failed(io::Error::new(
                io::ErrorKind::Unsupported,
                "no APFS filesystem is unmounted off macOS",
            )))
        }
    }

    /// Name the volume the held descriptor lies on (`fsetattrlist`, `ATTR_VOL_NAME`). The kernel
    /// names the mount of the descriptor's own vnode, never a path resolved again: once that
    /// volume is unmounted, even by force, the vnode is dead and the call fails (`ENOTSUP`,
    /// measured) rather than reaching the volume beneath. A path-based name (`setattrlist`,
    /// Disk Arbitration's rename) has no such binding: xnu names `vp->v_mount` of whatever the
    /// path resolves to, mount root or not, so a bare mountpoint names the host volume holding
    /// it. Only [`crate::OwnedVolumeRoot`] calls this, once the descriptor is proven to be an
    /// owned volume's root. A stated mount holds no descriptor and names nothing.
    pub(crate) fn set_volume_name(&self, name: &str) -> io::Result<()> {
        let Some(held) = self.held.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "a stated mount of {} holds no descriptor to name its volume through",
                    self.source_device
                ),
            ));
        };
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            let length = u32::try_from(name.len() + 1)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "volume name too long"))?;
            if name.as_bytes().contains(&0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "volume name contains NUL",
                ));
            }
            let mut request = libc::attrlist {
                bitmapcount: libc::ATTR_BIT_MAP_COUNT,
                reserved: 0,
                commonattr: 0,
                volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_NAME,
                dirattr: 0,
                fileattr: 0,
                forkattr: 0,
            };
            // One attrreference_t (i32 offset from itself, u32 length with the NUL), then the
            // name. xnu refuses a buffer that ends exactly at the name (`volname >= bufend -
            // offset - length` is EINVAL), so the buffer keeps a word of slack past it.
            let reference = std::mem::size_of::<libc::attrreference_t>();
            let mut buffer = vec![0u8; reference + name.len() + 1 + 4];
            buffer[..4].copy_from_slice(&(reference as i32).to_ne_bytes());
            buffer[4..8].copy_from_slice(&length.to_ne_bytes());
            buffer[reference..reference + name.len()].copy_from_slice(name.as_bytes());
            // SAFETY: `held` owns a live descriptor, `request` names one settable volume
            // attribute, and `buffer` is readable for the full length the kernel is told.
            let status = unsafe {
                libc::fsetattrlist(
                    held.as_raw_fd(),
                    (&raw mut request).cast(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    0,
                )
            };
            if status != 0 {
                return Err(io::Error::last_os_error());
            }
            unsafe extern "C" {
                fn notify_post(name: *const libc::c_char) -> u32;
            }
            // kNotifyVFSUpdate from notify_keys.h is Disk Arbitration's mount-update input.
            // The notification refreshes descriptions; it never names a volume through a path.
            let key = c"com.apple.system.kernel.mountupdate";
            // SAFETY: `key` is a static NUL-terminated string; notify_post retains no pointer.
            let notified = unsafe { notify_post(key.as_ptr()) };
            if notified != 0 {
                cowshed_os::eprintln!(
                    "cowshed: named APFS volume {} {name:?}, but notify_post({key:?}) returned status {notified}; Disk Arbitration may still display its previous name",
                    self.source_device
                );
            }
            Ok(())
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (held, name);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "no APFS volume is named off macOS",
            ))
        }
    }
}

#[cfg(target_os = "macos")]
fn descriptor_filesystem(held: &std::fs::File) -> io::Result<libc::statfs> {
    use std::os::fd::AsRawFd;

    // SAFETY: statfs is plain data, for which all zeroes is a value.
    let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: held owns a live descriptor and filesystem is writable for the complete record.
    if unsafe { libc::fstatfs(held.as_raw_fd(), &mut filesystem) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(filesystem)
}

/// Hold a directory's vnode and read its filesystem, never redirecting a later operation through
/// the pathname. Event-only custody preserves that identity without making our own unmount busy.
pub fn hold_mount(path: &std::path::Path) -> io::Result<HeldMount> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::{CStr, OsStr};
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        // std opens with O_CLOEXEC. Fence the descriptor immediately, including every failed
        // proof path, so no child still spawning can retain a temporary hold after its close.
        let held = cowshed_os::fork_lock::Fenced::new(
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_EVTONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(path)?,
        );
        let filesystem = descriptor_filesystem(&held)?;
        // SAFETY: Darwin NUL-terminates both names in a record this function owns.
        let source_device = unsafe { CStr::from_ptr(filesystem.f_mntfromname.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: same record ownership and NUL termination as f_mntfromname.
        let root = unsafe { CStr::from_ptr(filesystem.f_mntonname.as_ptr()) }.to_bytes();
        let here = held.metadata()?;
        let root = std::fs::metadata(OsStr::from_bytes(root))?;
        Ok(HeldMount {
            source_device,
            is_root: (here.dev(), here.ino()) == (root.dev(), root.ino()),
            filesystem_id: FilesystemId::from_native(filesystem.f_fsid),
            held: Some(held),
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("no APFS volume is mounted off macOS: {}", path.display()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stated_root_has_identity_but_no_native_unmount_authority() {
        let identity = FilesystemId::new([7, 26]);
        let held = HeldMount::stated("/dev/recorded", true, identity);
        assert_eq!(held.filesystem_id(), identity);
        assert_ne!(identity, FilesystemId::new([7, 27]));
        for mode in [UnmountMode::Normal, UnmountMode::ForceAfterGrace] {
            assert!(matches!(
                held.unmount(mode),
                Err(NativeUnmountError::Failed(source))
                    if source.kind() == io::ErrorKind::Unsupported
            ));
        }
    }

    #[test]
    fn kernel_mount_counts_refuse_errors_and_empty_tables() {
        for (count, mounts) in [(-1, None), (0, None), (1, Some(1))] {
            assert_eq!(mount_count(count), mounts, "getfsstat count={count}");
        }
    }

    /// A mountpoint nothing is mounted at is a plain directory of the volume beneath it: held, it
    /// answers that volume's device — the host's, for a directory under the temp root — and is
    /// not a mount root. The volume's own root answers the same filesystem as a root.
    ///
    /// The subject is read from a descriptor opened separately from the hold, through the raw
    /// `descriptor_filesystem` boundary, so no expected value comes from the `hold_mount` result
    /// under test: a hold of the wrong volume cannot select its own oracle. The kernel table is
    /// joined on the subject's FSID, never `st_dev`: an APFS volume group's siblings (the sealed
    /// system snapshot at `/` and Data, which the temp root is firmlinked onto) share one
    /// `st_dev`, so a device-number search answers whichever sibling the table lists first.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_held_mountpoint_nothing_is_mounted_at_answers_the_volume_beneath_it() {
        use std::ffi::{CStr, OsStr};
        use std::os::unix::ffi::OsStrExt;

        let directory = std::env::temp_dir().join(format!(
            "cowshed-unmounted-mountpoint-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let subject = std::fs::File::open(&directory).expect("open the bare mountpoint");
        let subject = descriptor_filesystem(&subject).expect("read the subject filesystem");
        let subject_id = FilesystemId::from_native(subject.f_fsid);
        // SAFETY: Darwin NUL-terminates both names in a record this snapshot owns.
        let source_device = unsafe { CStr::from_ptr(subject.f_mntfromname.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        let mut owning = kernel_filesystems()
            .expect("read the kernel mount table")
            .into_iter()
            .filter(|entry| FilesystemId::from_native(entry.f_fsid) == subject_id);
        let beneath = owning.next().expect("the mount owning the temp root");
        assert!(owning.next().is_none(), "one mount owns one FSID");
        // SAFETY: same NUL-termination and ownership as the subject record.
        let tabled_device = unsafe { CStr::from_ptr(beneath.f_mntfromname.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        assert_eq!(tabled_device, source_device);
        // SAFETY: same record ownership and NUL termination as the table's f_mntfromname.
        let mount_point = PathBuf::from(OsStr::from_bytes(
            unsafe { CStr::from_ptr(beneath.f_mntonname.as_ptr()) }.to_bytes(),
        ));

        let held = hold_mount(&directory).expect("hold the bare mountpoint");
        assert_eq!(held.filesystem_id(), subject_id);
        assert_eq!(held.source_device(), source_device.as_str());
        assert!(!held.is_root(), "a bare mountpoint is not a mount root");
        let root = hold_mount(&mount_point).expect("hold the volume's root");
        assert_eq!(root.filesystem_id(), subject_id);
        assert_eq!(root.source_device(), source_device.as_str());
        assert!(root.is_root());
        drop(held);
        std::fs::remove_dir(&directory).unwrap();
    }
}
