//! Linux mounts no APFS volume, so the image host's kernel questions have no answer here: each
//! refuses, naming the macOS facility that answers it.

use std::path::Path;

use super::ApfsStorageError;

/// A volume label is an APFS volume's; none is mounted on Linux.
pub(super) fn carries_label(_mount_point: &Path, _label: &str) -> Result<bool, ApfsStorageError> {
    Err(ApfsStorageError::Host(
        "reading an APFS volume's name requires macOS getattrlist".to_owned(),
    ))
}

/// An image operation's I/O window is measured around the macOS disk tools alone.
pub(super) fn own_io() -> Result<u64, String> {
    Err("APFS process-window I/O requires macOS proc_pid_rusage".into())
}
