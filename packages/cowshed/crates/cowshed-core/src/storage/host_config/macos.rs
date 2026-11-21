//! macOS host paths: the per-user mount root and the build-cache mountpoint live under one fixed
//! prefix in `/private/var/folders` (01a_apfs.md, "Stable workspace mount root").

use std::path::{Path, PathBuf};

use super::HostConfigError;

/// Stable macOS presentation root; numeric uid avoids home and temporary-directory aliases.
pub(crate) const MACOS_MOUNT_PREFIX: &str = "/private/var/folders/cowshed";

/// The build-cache mountpoint's parent, directly under [`MACOS_MOUNT_PREFIX`]: a mountpoint
/// needs a user-owned parent, so this one is the provisioning user's, group `nixbld`, 0750
/// (01c_macos.md, "`/private/var/folders` is root-owned and swept").
pub(crate) const BUILD_CACHE_PARENT: &str = "/private/var/folders/cowshed/shared";

/// Where the dedicated build-cache volume mounts, directly under [`BUILD_CACHE_PARENT`]
/// (01a_apfs.md, "Dedicated build-cache volume"), published to every workspace as
/// `COWSHED_BUILD_CACHE` (02_workspaces.md, "Workspace environment").
pub const BUILD_CACHE_MOUNTPOINT: &str = "/private/var/folders/cowshed/shared/build-cache";

pub(crate) fn canonical_mount_root(home: &Path) -> PathBuf {
    let _ = home;
    macos_mount_root(unsafe { libc::getuid() })
}

pub(crate) fn macos_mount_root(uid: u32) -> PathBuf {
    Path::new(MACOS_MOUNT_PREFIX)
        .join(uid.to_string())
        .join("mnt")
}

/// The invoking user's mount root for the production store.
pub(super) fn production_mount_root() -> Result<PathBuf, HostConfigError> {
    Ok(macos_mount_root(unsafe { libc::getuid() }))
}
