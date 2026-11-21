//! The volume Linux's substrates stat: the filesystem mounted exactly at a path, fenced by its
//! device and filesystem id, and the stat that reads it.
//!
//! A ZFS store's workspace and build volume are each a dataset mounted at its own root, so
//! `statvfs` on that root answers for the dataset alone: its blocks less its free blocks are the
//! bytes the dataset references (`referenced`), and its size is its `refquota`. ZFS publishes
//! both as of the pool's last transaction group, so a reading trails a write by at most that
//! interval. A plain-directory store's workspace is a directory on the filesystem that holds the
//! store; nothing is mounted at it, and its supervisor is configured with no volume
//! (`VolumeUnavailable::Unconfigured`) rather than answering for that filesystem.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use super::{VolumeAtSpawn, VolumeStat, VolumeStatError};
use crate::error::CowshedError;

/// A mounted filesystem by where it is mounted and which one it was when a job spawned: a later
/// mount at the same path is another volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostVolume {
    mount: PathBuf,
    identity: Identity,
}

/// The device number of the mount's root and the filesystem id the kernel reports for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    device: u64,
    filesystem: u64,
}

/// One read of the filesystem at a directory: which it is, and the bytes it uses.
struct Space {
    identity: Identity,
    used: u64,
}

impl HostVolume {
    /// The filesystem mounted exactly at `mount` and the bytes it uses now, both from one read.
    /// A directory on its parent's filesystem is refused.
    pub fn capture(mount: &Path) -> Result<(Self, u64), VolumeStatError> {
        let own = space(mount).map_err(|error| read_failed(mount, &error))?;
        let parent = mount.parent().ok_or_else(|| {
            failed(format_args!(
                "{} has no parent to be mounted on",
                mount.display()
            ))
        })?;
        let above = std::fs::metadata(parent).map_err(|error| read_failed(parent, &error))?;
        if above.dev() == own.identity.device {
            return Err(failed(format_args!(
                "no volume is mounted at {}",
                mount.display()
            )));
        }
        let volume = Self {
            mount: mount.to_path_buf(),
            identity: own.identity,
        };
        Ok((volume, own.used))
    }

    pub fn mount(&self) -> &Path {
        &self.mount
    }
}

/// The mounted filesystem's stat: the bytes an owned volume itself uses, never its pool's or a
/// walk of its tree.
#[derive(Clone, Copy, Debug, Default)]
pub struct HostVolumeStat;

impl VolumeStat for HostVolumeStat {
    type Volume = HostVolume;

    /// A mount that is no longer the one found at spawn is an error, never its successor's usage.
    fn used_bytes(&self, volume: &HostVolume) -> Result<u64, VolumeStatError> {
        let now = space(&volume.mount).map_err(|error| read_failed(&volume.mount, &error))?;
        if now.identity != volume.identity {
            return Err(failed(format_args!(
                "{} no longer holds the volume mounted there when the job spawned",
                volume.mount.display()
            )));
        }
        Ok(now.used)
    }
}

/// The volume mounted at `mount` as a job's spawn finds it: its identity and used bytes, or why
/// it has neither.
pub fn at_spawn(mount: &Path) -> VolumeAtSpawn<HostVolume> {
    VolumeAtSpawn::observed(HostVolume::capture(mount))
}

/// The filesystem `directory` is on, read through one descriptor so its identity and its usage
/// are of the same mount.
fn space(directory: &Path) -> io::Result<Space> {
    let opened = File::open(directory)?;
    let device = opened.metadata()?.dev();
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `opened` is an open descriptor and `stat` is a writable `statvfs`.
    if unsafe { libc::fstatvfs(opened.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fstatvfs` returned 0, so it filled `stat`.
    let stat = unsafe { stat.assume_init() };
    // `f_blocks` and `f_bfree` count `f_frsize` units; `f_bsize` is only the preferred I/O size
    // (a dataset's `recordsize`).
    let used = stat
        .f_blocks
        .checked_sub(stat.f_bfree)
        .and_then(|blocks| blocks.checked_mul(stat.f_frsize))
        .ok_or_else(|| {
            io::Error::other(format!(
                "the filesystem reports {} blocks of {} bytes with {} free",
                stat.f_blocks, stat.f_frsize, stat.f_bfree
            ))
        })?;
    Ok(Space {
        identity: Identity {
            device,
            filesystem: stat.f_fsid,
        },
        used,
    })
}

fn read_failed(path: &Path, error: &io::Error) -> VolumeStatError {
    failed(format_args!(
        "stat the filesystem at {}: {error}",
        path.display()
    ))
}

fn failed(cause: std::fmt::Arguments<'_>) -> VolumeStatError {
    CowshedError::environment_missing(
        format!("volume usage read failed: {cause}"),
        "cowshed doctor --json",
    )
    .into()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{HostVolume, HostVolumeStat};
    use crate::api::resources::{VolumeUnavailable, VolumeUsage};
    use crate::runtime::volume_usage::{VolumeBaseline, VolumeMountpoint, VolumeStat as _};

    /// `/proc` is a filesystem of its own on every Linux host, and it uses no blocks.
    #[test]
    fn a_filesystem_mounted_at_its_root_is_read_as_a_volume() {
        let (volume, used) = HostVolume::capture(Path::new("/proc")).expect("procfs is mounted");
        assert_eq!(volume.mount(), Path::new("/proc"));
        assert_eq!(used, 0);
        assert_eq!(
            HostVolumeStat.used_bytes(&volume).expect("the same mount"),
            0
        );
        let proc = VolumeMountpoint::new(PathBuf::from("/proc")).expect("procfs is mounted");
        let sample =
            VolumeBaseline::admitted(Some(&proc), Some(Path::new("/proc"))).sample(&HostVolumeStat);
        for usage in [Some(sample.workspace), sample.build] {
            let Some(VolumeUsage::Read { delta_bytes }) = usage else {
                panic!("a mounted filesystem is read: {usage:?}");
            };
            assert_eq!(delta_bytes.get(), 0);
        }
    }

    /// A build path that is a directory on its parent's filesystem answers for no volume: its
    /// usage is its own failure, and the workspace volume beside it is still read.
    #[test]
    fn a_directory_on_its_parents_filesystem_is_no_volume() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "cowshed-volume-usage",
        );
        let directory = root.join("build");
        std::fs::create_dir(&directory).expect("a plain directory");
        let proc = VolumeMountpoint::new(PathBuf::from("/proc")).expect("procfs is mounted");
        let sample =
            VolumeBaseline::admitted(Some(&proc), Some(&directory)).sample(&HostVolumeStat);
        assert!(matches!(sample.workspace, VolumeUsage::Read { .. }));
        let Some(VolumeUsage::Unavailable {
            reason: VolumeUnavailable::Failed { message },
        }) = sample.build
        else {
            panic!("a plain directory has no volume stat: {:?}", sample.build);
        };
        assert!(message.contains("no volume is mounted at"), "{message}");
    }

    /// A volume captured at one mount is not answered for by whatever is mounted elsewhere.
    #[test]
    fn a_mount_that_is_another_filesystem_is_not_the_captured_volume() {
        let (proc, _) = HostVolume::capture(Path::new("/proc")).expect("procfs is mounted");
        let (sys, _) = HostVolume::capture(Path::new("/sys")).expect("sysfs is mounted");
        let moved = HostVolume {
            mount: sys.mount,
            identity: proc.identity,
        };
        let error = HostVolumeStat
            .used_bytes(&moved)
            .expect_err("sysfs is not the captured procfs");
        assert!(
            error.to_string().contains("no longer holds the volume"),
            "{error}"
        );
    }
}
