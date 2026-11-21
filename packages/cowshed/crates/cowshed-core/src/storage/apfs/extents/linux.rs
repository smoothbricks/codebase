//! Linux keeps no RAW APFS image: a workspace payload is a ZFS dataset or a directory, and
//! neither has an extent map to count or rewrite. A file's data regions are counted one run
//! each, and every primitive of the image rewrite refuses.

use std::fs::File;
use std::io;
use std::path::Path;

use super::{ApfsStorageError, PhysicalRun, io_error};

/// Nothing maps a file range to the device here, so a data region is one run.
pub(super) fn run_bytes(_: &File, _: u64, length: u64) -> io::Result<u64> {
    Ok(length)
}

pub(super) fn physical_run(_: &File, _: u64, _: u64) -> io::Result<PhysicalRun> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image extents are read with macOS F_LOG2PHYS_EXT",
    ))
}

pub(super) fn punch_hole(_: &File, _: u64, _: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image holes are punched with macOS F_PUNCHHOLE",
    ))
}

/// A rewrite copies a whole image, and the recovery copy's Linux pass-through would put every byte
/// of it in the page cache: refuse, as every other rewrite primitive here does.
pub(super) fn bypass_cache(_: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image rewrites bypass the cache with macOS F_NOCACHE",
    ))
}

/// The room an image rewrite needs is asked for no image here.
pub(super) fn available_bytes(directory: &Path) -> Result<u64, ApfsStorageError> {
    Err(io_error(
        "read free space",
        directory,
        io::Error::new(
            io::ErrorKind::Unsupported,
            "an image rewrite's free space is read on macOS alone",
        ),
    ))
}
