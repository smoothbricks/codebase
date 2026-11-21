//! Linux's check that an owned tree holds no mount: every entry's mount identity (`statx`'s
//! `stx_mnt_id`) must be the one of the root's parent.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cowshed_os::fsio::{mount_id, open_directory_nofollow, open_path_nofollow};

/// Whether the root or a descendant is mounted independently of the root's parent.
/// Mount identities, not backing-device numbers, detect a root mount and bind mounts.
pub(super) fn tree_crosses_device(root: &Path) -> io::Result<Option<PathBuf>> {
    let parent = root.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "owned tree has no parent mount",
        )
    })?;
    let expected = mount_id(&open_directory_nofollow(parent)?)?;
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let held = open_directory_nofollow(&directory)?;
        if mount_id(&held)? != expected {
            return Ok(Some(directory));
        }
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            // A name alone pins directory, file and symlink entries without reading or following
            // their contents. A bind-mounted file must refuse before its siblings disappear.
            let entry = fs::File::from(open_path_nofollow(&path)?);
            if mount_id(&entry)? != expected {
                return Ok(Some(path));
            }
            if entry.metadata()?.is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(None)
}
