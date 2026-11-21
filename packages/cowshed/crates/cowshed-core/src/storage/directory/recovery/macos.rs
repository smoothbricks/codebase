//! macOS's check that an owned tree holds no mount: the kernel mount table names every mount
//! point, and none may lie at or below the tree's root.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Whether the root or a descendant is mounted independently of the root's parent.
pub(super) fn tree_crosses_device(root: &Path) -> io::Result<Option<PathBuf>> {
    let root = fs::canonicalize(root)?;
    Ok(cowshed_apfs::mounts::kernel_mounts()?
        .into_iter()
        .find(|mount| mount.mount_point.starts_with(&root))
        .map(|mount| mount.mount_point))
}
