//! Linux's answer to where a path's filesystem is mounted: the mount `statx` says the path is
//! on, found by its id in `/proc/self/mountinfo`. `statfs` names no mount point on Linux.

use std::fs::File;
use std::path::{Path, PathBuf};

/// Where the filesystem holding `path` is mounted. `None` for a path that cannot be reached, as
/// for a table that cannot be read or no longer holds the path's mount.
pub(super) fn mount_point_of(path: &Path) -> Option<PathBuf> {
    // A name alone reaches the entry without reading it, following a final symlink as `statfs`
    // does.
    let held = File::from(cowshed_os::fsio::open_path(path).ok()?);
    let id = cowshed_os::fsio::mount_id(&held).ok()?;
    crate::lost_mounts::linux::mounts()
        .ok()?
        .into_iter()
        .find(|mount| mount.id == id)
        .map(|mount| mount.mount_point)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_resolves_to_the_mount_that_holds_it() {
        assert_eq!(mount_point_of(Path::new("/")), Some(PathBuf::from("/")));
        // `/proc` is its own mount, and a path inside it resolves to that mount, not to itself.
        assert_eq!(
            mount_point_of(Path::new("/proc/self")),
            Some(PathBuf::from("/proc"))
        );
        assert_eq!(mount_point_of(Path::new("/nonexistent-cowshed-path")), None);
    }
}
