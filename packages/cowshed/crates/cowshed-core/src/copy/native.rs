#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;

/// Materialize one leaf and carry one directory's attributes, with the platform's clone where
/// its filesystem offers one.
pub use platform::{copy_leaf, directory_metadata};

/// Keep a descriptor's transfers out of the page cache where the platform has a per-descriptor
/// policy for it; a platform without one leaves the cache to carry them.
pub(crate) use platform::bypass_cache;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafMaterialization {
    Cloned,
    Copied,
}

/// Observe a logical hole in a freshly opened regular file: its first data region is not the
/// whole of `length`. Physical compression alone is not a hole: SEEK_HOLE answers in the logical
/// data stream. The descriptor is left at offset zero, where a fresh open put it, for a copier
/// that reads through the descriptor's offset; an unsupported observation is an error, never
/// guessed from st_blocks.
pub(super) fn has_holes(file: &File, length: u64) -> io::Result<bool> {
    if length == 0 {
        return Ok(false);
    }
    let walked = next_data_region(file, 0, length);
    // SAFETY: the same live descriptor the walk moved is restored to the start, after a failed
    // walk too.
    let restored = if unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_SET) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    let whole = matches!(walked?, Some((0, end)) if end == length);
    restored?;
    Ok(!whole)
}

/// The next `[start, end)` run of data at or after `offset` and before `length`, or `None` when
/// only a hole remains: SEEK_DATA finds it, SEEK_HOLE ends it, and ENXIO means no data follows.
/// Data past `length` was written after the caller's witness and lies outside the range it asked
/// about. Each call moves the descriptor's offset, so every reader of these regions transfers
/// positionally.
pub(crate) fn next_data_region(
    file: &File,
    offset: u64,
    length: u64,
) -> io::Result<Option<(u64, u64)>> {
    if offset >= length {
        return Ok(None);
    }
    let seek = |from: u64, whence| -> io::Result<Option<u64>> {
        let from = libc::off_t::try_from(from).map_err(io::Error::other)?;
        // SAFETY: the descriptor is live for the borrow of `file`; `lseek` only moves its offset.
        let found = unsafe { libc::lseek(file.as_raw_fd(), from, whence) };
        if found < 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ENXIO) => Ok(None),
                _ => Err(error),
            };
        }
        u64::try_from(found).map(Some).map_err(io::Error::other)
    };
    let Some(start) = seek(offset, libc::SEEK_DATA)?.filter(|&start| start < length) else {
        return Ok(None);
    };
    let end = seek(start, libc::SEEK_HOLE)?.unwrap_or(length).min(length);
    if start < offset || end <= start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid data region [{start}, {end}) seeking data from offset {offset}"),
        ));
    }
    Ok(Some((start, end)))
}
