#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "macos")]
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;
#[cfg(target_os = "macos")]
use std::os::fd::RawFd;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafMaterialization {
    Cloned,
    Copied,
}

#[cfg(any(not(target_os = "macos"), test))]
pub(super) fn clone_fallback_error(error: &std::io::Error) -> bool {
    error.raw_os_error().is_some_and(|code| {
        code == libc::EXDEV
            || code == libc::EOPNOTSUPP
            || code == libc::ENOTTY
            || code == libc::EINVAL
    })
}

#[cfg(not(target_os = "macos"))]
pub use linux::{copy_leaf, directory_metadata};
#[cfg(target_os = "macos")]
pub use macos::{copy_leaf, directory_metadata};

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

/// Keep this descriptor's transfers out of the page cache. A copy or a verification passes each
/// byte through once, so caching it only evicts what the rest of the host is using: a 300 GB
/// recovery image copied through the cache left 56 GiB in it and swapped the host. macOS
/// `F_NOCACHE` is a per-descriptor policy that later reads and writes on it follow.
#[cfg(target_os = "macos")]
pub(crate) fn bypass_cache(file: &File) -> io::Result<()> {
    // SAFETY: `F_NOCACHE` takes an int flag and only changes the descriptor's caching policy.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Linux has no per-descriptor uncached mode short of `O_DIRECT`, which binds every buffer,
/// offset and length to the device's logical block alignment; these transfers keep no such
/// contract. The page cache carries them, and this changes nothing.
#[cfg(not(target_os = "macos"))]
pub(crate) fn bypass_cache(_: &File) -> io::Result<()> {
    Ok(())
}

/// Retain the native cause and descriptor context.
#[cfg(target_os = "macos")]
#[derive(Debug, thiserror::Error)]
#[error("{operation} on descriptor {descriptor}, attribute {attribute:?}: {source}")]
struct MetadataError {
    operation: &'static str,
    descriptor: RawFd,
    attribute: Option<CString>,
    #[source]
    source: io::Error,
}

#[cfg(target_os = "macos")]
fn metadata_error(
    file: &File,
    operation: &'static str,
    attribute: Option<&CStr>,
    source: io::Error,
) -> io::Error {
    io::Error::new(
        source.kind(),
        MetadataError {
            operation,
            descriptor: file.as_raw_fd(),
            attribute: attribute.map(CStr::to_owned),
            source,
        },
    )
}
#[cfg(test)]
mod tests {
    use super::clone_fallback_error;

    #[test]
    fn only_filesystem_clone_capability_errors_fall_back() {
        for code in [libc::EXDEV, libc::EOPNOTSUPP, libc::ENOTTY, libc::EINVAL] {
            assert!(clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
        for code in [libc::EIO, libc::ENOSPC, libc::EACCES] {
            assert!(!clone_fallback_error(&std::io::Error::from_raw_os_error(
                code
            )));
        }
    }
}
