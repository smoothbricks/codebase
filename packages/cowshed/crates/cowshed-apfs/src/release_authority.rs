//! Release authority (01a_apfs.md, "Release authority"): Disk Arbitration unmounts or ejects a
//! disk without asking authd only for the disk's owner or root.
//!
//! diskarbitrationd takes a disk's owner from the first `owner-uid` above its IOMedia, which both
//! image drivers record as the attaching uid, and takes root when none is recorded. The client's
//! `_DAAuthorize` passes an unmount or eject outright when that owner is the caller's euid;
//! otherwise it asks authd for `system.volume.<kind>.unmount` with interaction allowed, which root
//! passes (`is-root`) and a non-administrator answers only at a SecurityAgent prompt that holds the
//! request until someone answers it. `hdiutil detach`, forced or not, unmounts through that same
//! call first.
//!
//! `hdiutil detach` waits on that prompt inside its own `DADiskUnmount`. `diskutil eject`, `unmount`
//! and `unmountDisk` go through DiskManagement and finish, yet launch SecurityAgent on the way
//! (measured 2026-10-22, 01a). Both are refused for a disk-image disk another user owns: no cowshed
//! verb raises a prompt (06_cli.md). The host's own disks have no image drive above them and are
//! not this check's.
//!
//! Every disk child spawns through [`crate::SystemCommandRunner`], cowshed's own detaches and
//! `cowshed disk-command` alike, so the check runs there, inside the storage lease and immediately
//! before the spawn: a release of a disk-image disk another user owns is refused as
//! [`ReleaseRefusal::NeedsAuthorization`]. A release whose target names no disk this check can
//! resolve (a volume name, a UUID, a path off any disk) is refused as
//! [`ReleaseRefusal::Unresolved`]: nothing says it is not another user's image.

use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use crate::{CommandRequest, CommandRunError, DISKUTIL, HDIUTIL};

/// What a Disk Arbitration release names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseTarget {
    /// A disk by its BSD name (`disk9`, `disk9s1`), however the request spelled its device node.
    Disk(String),
    /// An absolute path: the release takes the volume mounted there.
    Path(PathBuf),
}

/// Why a release is refused before it spawns.
#[derive(Debug)]
pub enum ReleaseRefusal {
    /// `owner_uid` owns `disk`, and Disk Arbitration would ask authd before releasing it for
    /// `euid`.
    NeedsAuthorization {
        disk: String,
        owner_uid: libc::uid_t,
        euid: libc::uid_t,
    },
    /// The release names no single disk or absolute path, so its owner cannot be read.
    Unresolved(String),
    /// The path names no disk-backed volume, so its owner cannot be read.
    NoDisk { path: PathBuf, source: io::Error },
    /// The kernel registry could not say who owns `disk`.
    OwnerUnread { disk: String, source: io::Error },
}

impl ReleaseRefusal {
    /// What lets the release run.
    pub fn hint(&self) -> String {
        match self {
            Self::NeedsAuthorization {
                disk, owner_uid: 0, ..
            } => format!(
                "run it as root, the user that attached /dev/{disk} (prefix it with `sudo`)"
            ),
            Self::NeedsAuthorization {
                disk, owner_uid, ..
            } => format!("run it as uid {owner_uid}, the user that attached /dev/{disk}"),
            Self::Unresolved(_) | Self::NoDisk { .. } => {
                "name the disk by its device node (/dev/diskN or /dev/diskNsM)".to_owned()
            }
            Self::OwnerUnread { .. } => "cowshed doctor".to_owned(),
        }
    }
}

impl fmt::Display for ReleaseRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeedsAuthorization {
                disk,
                owner_uid,
                euid,
            } => write!(
                f,
                "ReleaseNeedsAuthorization: /dev/{disk} was attached by uid {owner_uid}; Disk \
                 Arbitration releases it without an administrator prompt only for uid \
                 {owner_uid} or root, so it is not released as uid {euid}"
            ),
            Self::Unresolved(target) => write!(
                f,
                "ReleaseNeedsAuthorization: {target:?} names no disk device or absolute path, so \
                 nothing says Disk Arbitration would release it without an administrator prompt"
            ),
            Self::NoDisk { path, source } => write!(
                f,
                "ReleaseNeedsAuthorization: {} is on no disk device ({source}), so nothing says \
                 Disk Arbitration would release it without an administrator prompt",
                path.display()
            ),
            Self::OwnerUnread { disk, source } => write!(
                f,
                "ReleaseNeedsAuthorization: the owner of /dev/{disk} could not be read ({source}), \
                 so nothing says Disk Arbitration would release it without an administrator prompt"
            ),
        }
    }
}

impl std::error::Error for ReleaseRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoDisk { source, .. } | Self::OwnerUnread { source, .. } => Some(source),
            Self::NeedsAuthorization { .. } | Self::Unresolved(_) => None,
        }
    }
}

/// The one decision: Disk Arbitration passes a release outright for the disk's owner, and authd
/// passes root (`is-root`); anyone else waits on an administrator prompt.
pub fn refuse_unowned(
    disk: &str,
    owner_uid: libc::uid_t,
    euid: libc::uid_t,
) -> Result<(), ReleaseRefusal> {
    if euid == 0 || owner_uid == euid {
        Ok(())
    } else {
        Err(ReleaseRefusal::NeedsAuthorization {
            disk: disk.to_owned(),
            owner_uid,
            euid,
        })
    }
}

/// What `request` releases through Disk Arbitration: `None` when it releases nothing, `Some(Err)`
/// when it is a release that names no single target. `hdiutil detach|eject [-flag…] <target>`;
/// `diskutil eject|unmount|umount|unmountDisk|umountDisk [force] <target>`, its verb in any case.
pub fn release_target(request: &CommandRequest) -> Option<Result<ReleaseTarget, ReleaseRefusal>> {
    let (verb, rest) = request.args.split_first()?;
    let operands = if request.program == Path::new(HDIUTIL) {
        if verb != "detach" && verb != "eject" {
            return None;
        }
        rest
    } else if request.program == Path::new(DISKUTIL) {
        if !["eject", "unmount", "umount", "unmountDisk", "umountDisk"]
            .iter()
            .any(|release| verb.eq_ignore_ascii_case(release))
        {
            return None;
        }
        match rest {
            [force, operands @ ..] if force.eq_ignore_ascii_case("force") => operands,
            operands => operands,
        }
    } else {
        return None;
    };
    // hdiutil's flags (`-force`, `-verbose`, …) are not targets; no disk or path starts with `-`.
    let mut targets = operands
        .iter()
        .filter(|arg| !arg.as_bytes().starts_with(b"-"));
    Some(match (targets.next(), targets.next()) {
        (Some(target), None) => target_of(target),
        _ => Err(ReleaseRefusal::Unresolved(
            request
                .args
                .iter()
                .map(|arg| arg.to_string_lossy())
                .collect::<Vec<_>>()
                .join(" "),
        )),
    })
}

/// A release operand as the disk or path it names: a device node or bare name in
/// [`crate::device`]'s one grammar, or an absolute path.
fn target_of(operand: &OsStr) -> Result<ReleaseTarget, ReleaseRefusal> {
    let unresolved = || ReleaseRefusal::Unresolved(operand.to_string_lossy().into_owned());
    let text = operand.to_str().ok_or_else(unresolved)?;
    let node = text
        .strip_prefix("/dev/r")
        .or_else(|| text.strip_prefix("/dev/"));
    if crate::device::identifier_depth(node.unwrap_or(text)).is_some() {
        Ok(ReleaseTarget::Disk(node.unwrap_or(text).to_owned()))
    } else if node.is_none() && text.starts_with('/') {
        Ok(ReleaseTarget::Path(PathBuf::from(operand)))
    } else {
        Err(unresolved())
    }
}

/// The BSD disk name of the volume mounted at or holding `path`.
#[cfg(target_os = "macos")]
fn disk_of_path(path: &Path) -> Result<String, ReleaseRefusal> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;
    let no_disk = |source| ReleaseRefusal::NoDisk {
        path: path.to_owned(),
        source,
    };
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| no_disk(io::Error::from(io::ErrorKind::InvalidInput)))?;
    // SAFETY: statfs is plain data, for which all zeroes is a value.
    let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is NUL-terminated and `filesystem` is writable.
    if unsafe { libc::statfs(c_path.as_ptr(), &mut filesystem) } != 0 {
        return Err(no_disk(io::Error::last_os_error()));
    }
    // SAFETY: the kernel NUL-terminates `f_mntfromname` within its buffer.
    let source = unsafe { CStr::from_ptr(filesystem.f_mntfromname.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    match source.strip_prefix("/dev/") {
        Some(node) if crate::device::identifier_depth(node).is_some() => Ok(node.to_owned()),
        _ => Err(no_disk(io::Error::other(format!(
            "its file system is mounted from {source}"
        )))),
    }
}

/// Refuse `request` before it spawns when it is a Disk Arbitration release of a disk-image disk
/// this process would need authorization to release, or one whose disk cannot be named or read;
/// every other command passes. A disk with no image drive above it is the host's own (the store
/// volume bootstrap remounts with an unprivileged `diskutil unmount force`) and passes untouched,
/// as does a disk the registry no longer has, whose absence the tool reports itself.
#[cfg(target_os = "macos")]
pub(crate) fn refuse_unauthorized_release(request: &CommandRequest) -> Result<(), CommandRunError> {
    let refused = |refusal| CommandRunError {
        request: request.clone(),
        failure: crate::CommandRunFailure::Release(refusal),
    };
    let Some(target) = release_target(request) else {
        return Ok(());
    };
    let disk = match target.map_err(refused)? {
        ReleaseTarget::Disk(disk) => disk,
        ReleaseTarget::Path(path) => disk_of_path(&path).map_err(refused)?,
    };
    match crate::io_registry::disk_image_owner_uid(&disk) {
        Ok(None) => Ok(()),
        Ok(Some(owner_uid)) => {
            refuse_unowned(&disk, owner_uid, crate::current_euid()).map_err(refused)
        }
        Err(source) => Err(refused(ReleaseRefusal::OwnerUnread { disk, source })),
    }
}

/// No other kernel has Disk Arbitration: nothing to refuse.
#[cfg(not(target_os = "macos"))]
pub(crate) fn refuse_unauthorized_release(_: &CommandRequest) -> Result<(), CommandRunError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(program: &str, args: &[&str]) -> Option<Result<ReleaseTarget, String>> {
        release_target(&CommandRequest::new(program, args.iter().copied()))
            .map(|target| target.map_err(|refusal| refusal.to_string()))
    }

    fn disk(name: &str) -> Option<Result<ReleaseTarget, String>> {
        Some(Ok(ReleaseTarget::Disk(name.to_owned())))
    }

    /// Every Disk Arbitration release names its disk, however it is spelled; nothing else is a
    /// release.
    #[test]
    fn each_release_verb_names_its_disk() {
        for (program, args, expected) in [
            (HDIUTIL, &["detach", "/dev/disk9"][..], disk("disk9")),
            (
                HDIUTIL,
                &["detach", "-verbose", "/dev/disk9"],
                disk("disk9"),
            ),
            (
                HDIUTIL,
                &["detach", "-force", "-verbose", "/dev/rdisk9"],
                disk("disk9"),
            ),
            (HDIUTIL, &["eject", "disk9s1"], disk("disk9s1")),
            (
                HDIUTIL,
                &["detach", "/Volumes/work"],
                Some(Ok(ReleaseTarget::Path("/Volumes/work".into()))),
            ),
            (DISKUTIL, &["eject", "disk9"], disk("disk9")),
            (DISKUTIL, &["eject", "force", "/dev/disk9"], disk("disk9")),
            (DISKUTIL, &["unmount", "/dev/disk9s1"], disk("disk9s1")),
            (DISKUTIL, &["unmount", "force", "disk9s1"], disk("disk9s1")),
            (DISKUTIL, &["umount", "disk9s1"], disk("disk9s1")),
            (DISKUTIL, &["unmountDisk", "force", "disk9"], disk("disk9")),
            (DISKUTIL, &["UNMOUNTDISK", "disk9"], disk("disk9")),
            (DISKUTIL, &["umountDisk", "/dev/disk9"], disk("disk9")),
            (HDIUTIL, &["attach", "-kernel", "/x.dmg"], None),
            (HDIUTIL, &["info", "-plist"], None),
            (DISKUTIL, &["mount", "disk9s1"], None),
            (DISKUTIL, &["image", "info", "/x.dmg"], None),
            (DISKUTIL, &["info", "-plist", "disk9"], None),
            ("/sbin/umount", &["/Volumes/work"], None),
        ] {
            assert_eq!(target(program, args), expected, "{program} {args:?}");
        }
    }

    /// A release that names no single disk or absolute path is refused: its owner is unknown.
    #[test]
    fn a_release_naming_no_disk_or_path_is_unresolved() {
        for (program, args) in [
            (DISKUTIL, &["unmount", "work"][..]),
            (DISKUTIL, &["eject", "0F2C9A3E-1A2B-4C5D-8E9F-001122334455"]),
            (DISKUTIL, &["unmountDisk"]),
            (HDIUTIL, &["detach"]),
            (HDIUTIL, &["detach", "/dev/disk9", "/dev/disk10"]),
            (HDIUTIL, &["detach", "disk9x"]),
            // device.rs's grammar: the kernel never zero-pads a unit or slice.
            (HDIUTIL, &["detach", "/dev/disk09"]),
            (DISKUTIL, &["unmount", "disk9s01"]),
        ] {
            assert!(
                matches!(target(program, args), Some(Err(_))),
                "{program} {args:?}"
            );
        }
    }

    /// Disk Arbitration passes a release outright for the disk's owner; authd passes root;
    /// anyone else waits on an administrator prompt.
    #[test]
    fn release_authority_is_the_attaching_user_or_root() {
        for (owner_uid, euid, refused) in [
            (502, 502, false),
            (0, 0, false),
            (502, 0, false),
            (0, 502, true),
            (501, 502, true),
        ] {
            let result = refuse_unowned("disk9", owner_uid, euid);
            assert_eq!(result.is_err(), refused, "owner {owner_uid}, euid {euid}");
        }
        let refusal = refuse_unowned("disk9", 0, 502).unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "ReleaseNeedsAuthorization: /dev/disk9 was attached by uid 0; Disk Arbitration \
             releases it without an administrator prompt only for uid 0 or root, so it is not \
             released as uid 502"
        );
        assert_eq!(
            refusal.hint(),
            "run it as root, the user that attached /dev/disk9 (prefix it with `sudo`)"
        );
    }
}
