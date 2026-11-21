//! Linux host paths: the per-user mount root is in the user's home, and the build cache has a
//! fixed host path beside the store (09_substrates.md, "Linux host paths").

use std::path::{Path, PathBuf};

use super::{HostConfigError, validate_absolute_path};

/// Where the build cache lives: the `<root>/build-cache` dataset's mountpoint on a ZFS host, a
/// directory of the store's filesystem on a directory-store host. Published to every workspace
/// as `COWSHED_BUILD_CACHE` (02_workspaces.md, "Workspace environment").
pub const BUILD_CACHE_MOUNTPOINT: &str = "/private/cowshed/build-cache";

/// `~/.cowshed/mnt` (01_storage.md, "Directory layout").
pub(crate) fn canonical_mount_root(home: &Path) -> PathBuf {
    home.join(".cowshed/mnt")
}

/// The invoking user's mount root for the production store, from the `HOME` it runs with.
pub(super) fn production_mount_root() -> Result<PathBuf, HostConfigError> {
    let home = std::env::var_os("HOME").ok_or(HostConfigError::HomeUnavailable)?;
    let home = PathBuf::from(home);
    validate_absolute_path(&home)?;
    Ok(canonical_mount_root(&home))
}
