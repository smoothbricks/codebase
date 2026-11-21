//! What the image host asks of the macOS kernel itself: a mounted volume's name and its used
//! bytes (`getattrlist`) and this process's own disk writes (`proc_pid_rusage`); and the clones
//! only the image host's `RuntimeStorageHost` asks for.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_apfs::CommandRunner;

use super::{ApfsStorageError, MacOsApfsExecutionHost, io_error};
use crate::build_volume::{BuildVolumeId, BuildVolumeLayout, Registration};

/// Whether the volume mounted at `mount_point` already carries `label`, read straight from the
/// kernel (`getattrlist`). A read that raced an unmount costs only a rename that the held root's
/// proof then refuses.
pub(super) fn carries_label(mount_point: &Path, label: &str) -> Result<bool, ApfsStorageError> {
    cowshed_apfs::volume_name(mount_point)
        .map(|name| name == label)
        .map_err(|error| io_error("read volume name", mount_point, error))
}

/// The bytes this process has written to disk so far: what an image operation's I/O window is
/// the difference of.
pub(super) fn own_io() -> Result<u64, String> {
    crate::runtime::process_usage::own_process_write_bytes().map_err(|error| error.to_string())
}

/// The clones only the image host's `RuntimeStorageHost` asks for
/// (`runtime/storage_host/macos.rs`).
impl<R> MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    /// Quiet retains the stopped supervisor's socket lease against replacement admission and
    /// the landing's shared GC hold. Taking a second exclusive claim would conflict with that
    /// hold; upgrading it would open an admission gap (16_build_volumes.md, Land step 4).
    pub(crate) fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &crate::runtime::build_volumes::Quiet,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<Duration>, ApfsStorageError> {
        let source = quiet.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            registration,
            mounted.then_some(mount.as_path()),
        )
    }

    /// Snapshot retained state under existing shared custody, without claiming or stopping its
    /// writers. The caller's lifecycle/image lock excludes adoption and the hold excludes release.
    pub(crate) fn clone_shared_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &crate::runtime::build_volumes::HeldVolume,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<Duration>, ApfsStorageError> {
        let source = source.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            registration,
            mounted.then_some(mount.as_path()),
        )
    }
}

/// One mounted volume a job owns: the mountpoint it was found at and the filesystem id the
/// kernel gave that mount. The id, not the volume UUID, is its identity: every image cloned from
/// one template or seed carries the same volume UUID, and a mountpoint with nothing mounted on it
/// answers for its parent's volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsVolume {
    mount: PathBuf,
    fsid: [i32; 2],
}

impl ApfsVolume {
    /// The volume mounted exactly at `mount` and the bytes it uses now, both from one read. A
    /// directory on its parent's volume is refused.
    pub fn capture(
        mount: &Path,
    ) -> Result<(Self, u64), crate::runtime::volume_usage::VolumeStatError> {
        let own = volume_space(mount).map_err(usage_error)?;
        let parent = mount.parent().ok_or_else(|| {
            usage_error(ApfsStorageError::Host(format!(
                "{} has no parent to be mounted on",
                mount.display()
            )))
        })?;
        if volume_space(parent).map_err(usage_error)?.fsid == own.fsid {
            return Err(usage_error(ApfsStorageError::Host(format!(
                "no volume is mounted at {}",
                mount.display()
            ))));
        }
        let volume = Self {
            mount: mount.to_path_buf(),
            fsid: own.fsid,
        };
        Ok((volume, own.used))
    }

    pub fn mount(&self) -> &Path {
        &self.mount
    }
}

/// The APFS volume stat: the bytes an owned volume itself uses, never its container's or a walk
/// of its tree.
#[derive(Clone, Copy, Debug, Default)]
pub struct ApfsVolumeStat;

impl crate::runtime::volume_usage::VolumeStat for ApfsVolumeStat {
    type Volume = ApfsVolume;

    /// A mount that is no longer the one found at spawn is an error, never its successor's usage.
    fn used_bytes(
        &self,
        volume: &ApfsVolume,
    ) -> Result<u64, crate::runtime::volume_usage::VolumeStatError> {
        let space = volume_space(&volume.mount).map_err(usage_error)?;
        if space.fsid != volume.fsid {
            return Err(usage_error(ApfsStorageError::Host(format!(
                "{} no longer holds the volume mounted there when the job spawned",
                volume.mount.display()
            ))));
        }
        Ok(space.used)
    }
}

fn usage_error(error: ApfsStorageError) -> crate::runtime::volume_usage::VolumeStatError {
    crate::CowshedError::environment_missing(
        format!("volume usage read failed: {error}"),
        "cowshed doctor --json",
    )
    .into()
}

#[derive(Debug, PartialEq)]
pub(super) struct VolumeSpace {
    pub(super) fsid: [i32; 2],
    pub(super) used: u64,
}

/// The `getattrlist` answer [`volume_space`] asks for: its length (u32), the attributes it holds
/// (attribute_set_t: five u32), the filesystem id (fsid_t: two i32), then the used bytes
/// (off_t), packed on 4-byte boundaries in attribute-group order.
pub(super) const VOLUME_SPACE_LENGTH: usize = 4 + 20 + 8 + 8;

/// The filesystem id of the mount holding `path` and the bytes its volume uses, from one
/// `getattrlist`. `statfs` is no substitute: on APFS its block counts are the container's, which
/// every sibling volume moves.
fn volume_space(path: &Path) -> Result<VolumeSpace, ApfsStorageError> {
    const OPERATION: &str = "read the volume attributes of";
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| ApfsStorageError::Host(format!("{} contains NUL", path.display())))?;
    let mut request = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_RETURNED_ATTRS | libc::ATTR_CMN_FSID,
        volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_SPACEUSED,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    #[repr(C, align(4))]
    struct Answer([u8; VOLUME_SPACE_LENGTH]);
    let mut answer = Answer([0; VOLUME_SPACE_LENGTH]);
    // SAFETY: `c_path` is NUL-terminated, `request` names only the attributes laid out in
    // `VOLUME_SPACE_LENGTH`, and `answer` is writable for its full length, which is what the
    // kernel is told.
    let status = unsafe {
        libc::getattrlist(
            c_path.as_ptr(),
            (&raw mut request).cast(),
            answer.0.as_mut_ptr().cast(),
            VOLUME_SPACE_LENGTH,
            libc::FSOPT_NOFOLLOW,
        )
    };
    if status != 0 {
        return Err(io_error(OPERATION, path, io::Error::last_os_error()));
    }
    decode_volume_space(&answer.0).map_err(|error| io_error(OPERATION, path, error))
}

/// A file system that does not report a requested attribute omits it from the answer; a negative
/// `off_t` is no byte count. Both are errors, never a zero.
pub(super) fn decode_volume_space(bytes: &[u8; VOLUME_SPACE_LENGTH]) -> io::Result<VolumeSpace> {
    let word = |at: usize| [bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]];
    if u32::from_ne_bytes(word(4)) & libc::ATTR_CMN_FSID == 0
        || u32::from_ne_bytes(word(8)) & libc::ATTR_VOL_SPACEUSED == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the file system reports no filesystem id or used bytes",
        ));
    }
    if usize::try_from(u32::from_ne_bytes(word(0))).ok() != Some(VOLUME_SPACE_LENGTH) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the volume attributes answer has an unexpected length",
        ));
    }
    let used = i64::from_ne_bytes([
        bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
    ]);
    Ok(VolumeSpace {
        fsid: [i32::from_ne_bytes(word(24)), i32::from_ne_bytes(word(28))],
        used: u64::try_from(used).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the volume reports {used} used bytes"),
            )
        })?,
    })
}
